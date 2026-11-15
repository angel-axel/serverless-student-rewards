"""
Buzón - Student Builder Group rewards platform

Lambda detrás de API Gateway (HTTP API, payload 2.0) para el buzón de sugerencias y los
mensajes al AWS Student Builder Group Leader.

Rutas:
    POST  /suggestions          miembro         enviar una sugerencia (con nombre o anónima)
    GET   /admin/suggestions    administrador   sugerencias paginadas: ?limit=&cursor=
    POST  /messages             miembro         enviar un mensaje al líder
    GET   /admin/messages       administrador   mensajes paginados: ?limit=&cursor=

Mensajes: siempre llevan el correo del estudiante (para poder responderle). Además de
guardarse, se reenvían con SES al correo del líder con Reply-To del estudiante, así que
basta con responder desde el correo. Si el reenvío falla, el mensaje igual queda en el
panel.

Anonimato: una sugerencia anónima no guarda el correo en ningún atributo, así que ni
siquiera desde la consola de DynamoDB se puede saber quién la envió.

XSS: el texto se guarda como texto plano. Antes se eliminan caracteres de control y se
rechaza cualquier contenido con forma de HTML o script (etiquetas, javascript:, atributos
on...=). La SPA además escapa todo lo que muestra, así que es una doble barrera.

Límite de envíos: cada estudiante puede enviar DAILY_LIMIT sugerencias y DAILY_LIMIT
mensajes al día. Cada contador vive en un item aparte (USER#<correo> / SUGGESTIONS#<fecha>
o MESSAGES#<fecha>) que solo guarda el número de envíos, no su contenido, y que DynamoDB
borra solo con TTL.

La autenticación la resuelve el autorizador JWT de la HTTP API. Aquí solo se valida
el grupo de Cognito de cada ruta. CORS se configura en la HTTP API.

Requiere la capa AWSLambdaPowertoolsPythonV3-python313-<arquitectura>.

Variables de entorno (todas obligatorias):
    TABLE_NAME     tabla de DynamoDB
    LEADER_EMAIL   correo del líder que recibe los mensajes
    SENDER_EMAIL   remitente verificado en SES. Ej.: avisos@example.com
"""

import base64
import html
import json
import os
import re
import unicodedata
import uuid
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from aws_lambda_powertools import Logger
from aws_lambda_powertools.event_handler import APIGatewayHttpResolver, Response, content_types
from aws_lambda_powertools.event_handler.exceptions import (
    BadRequestError,
    ForbiddenError,
    NotFoundError,
    ServiceError,
)
from aws_lambda_powertools.event_handler.middlewares import NextMiddleware
from aws_lambda_powertools.logging import correlation_paths
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]
LEADER_EMAIL = os.environ["LEADER_EMAIL"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]

ADMIN_GROUP = "administrador"
MEMBER_GROUP = "miembro"

# Correo institucional con número de control antes de la @
INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()
STUDENT_EMAIL = re.compile(r"^(?=[^@]*\d)[a-z0-9._-]+@" + re.escape(INSTITUTIONAL_DOMAIN) + "$")

TEXT_MIN, TEXT_MAX = 5, 1000
SUBJECT_MIN, SUBJECT_MAX = 3, 120
MESSAGE_MIN, MESSAGE_MAX = 10, 1000

# Contenido con forma de HTML o script: etiquetas (<b>, </div>, <!--, <script; el navegador
# no las reconoce si hay un espacio después de <, así que "a < b" sí se permite),
# esquemas peligrosos y atributos de evento (onerror=, onclick=...)
UNSAFE_TEXT = re.compile(
    r"</?[a-z!?]|javascript\s*:|vbscript\s*:|data\s*:\s*text/html|\bon[a-z]+\s*=",
    re.IGNORECASE,
)
# Caracteres de control (se conservan el salto de línea y el tabulador)
CONTROL_CHARS = re.compile(r"[\x00-\x08\x0b-\x1f\x7f\u200b-\u200f\u2028-\u202e\u2060-\u2064\ufeff]")
DAILY_LIMIT = 5

# Centro de México: UTC-6 todo el año; el límite diario se reinicia a medianoche local
MX_TZ = timezone(timedelta(hours=-6))

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # UTC; ordenable como texto en GSI1SK

PARTITION = "SUGGESTIONS"
MESSAGES_PARTITION = "MESSAGES"
PAGE_SIZE_MAX = 50

logger = Logger(service="sugerencias")

# strip_prefixes permite servir la API detrás de CloudFront en /api/*
app = APIGatewayHttpResolver(strip_prefixes=["/api"])

table = boto3.resource("dynamodb").Table(TABLE_NAME)
ses = boto3.client("sesv2")


class TooManyRequestsError(ServiceError):
    """429: se alcanzó el límite diario de sugerencias."""

    def __init__(self, msg):
        super().__init__(429, msg)


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #

def clean(value):
    """Convierte los Decimal de DynamoDB a int/float; Powertools los serializa como texto."""
    if isinstance(value, list):
        return [clean(v) for v in value]
    if isinstance(value, dict):
        return {k: clean(v) for k, v in value.items()}
    if isinstance(value, Decimal):
        return int(value) if value % 1 == 0 else float(value)
    return value


def pick(item, fields):
    """Regresa solo los campos indicados que existan en el item."""
    return {k: item[k] for k in fields if k in item}


def now_iso():
    return datetime.now(timezone.utc).strftime(ISO_FORMAT)


def mask(email):
    """Oculta el correo en los logs: l00000000@... -> l0***@..."""
    local, _, domain = (email or "").partition("@")
    return f"{local[:2]}***@{domain}" if domain else "desconocido"


def body():
    """Cuerpo de la petición como dict; 400 si no es un objeto JSON válido."""
    try:
        data = app.current_event.json_body
    except (json.JSONDecodeError, TypeError):
        raise BadRequestError("El cuerpo de la solicitud no es JSON válido.")
    if data is None:
        return {}
    if not isinstance(data, dict):
        raise BadRequestError("El cuerpo de la solicitud debe ser un objeto JSON.")
    return data


def encode_cursor(last_key):
    """LastEvaluatedKey -> texto opaco para la SPA."""
    if not last_key:
        return None
    return base64.urlsafe_b64encode(json.dumps(last_key, separators=(",", ":")).encode()).decode().rstrip("=")


def decode_cursor(cursor, partition):
    """Texto de la SPA -> ExclusiveStartKey. Debe pertenecer a la misma partición que se consulta."""
    if not cursor:
        return None
    try:
        raw = base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4))
        key = json.loads(raw)
    except (ValueError, json.JSONDecodeError):
        raise BadRequestError("El cursor no es válido.")
    expected = {"PK", "SK", "GSI1PK", "GSI1SK"}
    if (not isinstance(key, dict) or set(key) != expected
            or not all(isinstance(v, str) for v in key.values()) or key["GSI1PK"] != partition):
        raise BadRequestError("El cursor no es válido.")
    return key


def page_query(partition):
    """Una página de GSI1 (más recientes primero) según ?limit= y ?cursor=."""
    ev = app.current_event
    try:
        limit = int(ev.get_query_string_value("limit", "20"))
    except ValueError:
        raise BadRequestError("limit debe ser un número entero.")
    if not 1 <= limit <= PAGE_SIZE_MAX:
        raise BadRequestError(f"limit debe estar entre 1 y {PAGE_SIZE_MAX}.")

    kwargs = {
        "IndexName": "GSI1",
        "KeyConditionExpression": Key("GSI1PK").eq(partition),
        "ScanIndexForward": False,
        "Limit": limit,
    }
    start = decode_cursor(ev.get_query_string_value("cursor", ""), partition)
    if start:
        kwargs["ExclusiveStartKey"] = start
    res = table.query(**kwargs)
    return res["Items"], encode_cursor(res.get("LastEvaluatedKey"))


# --------------------------------------------------------------------------- #
# Autorización por grupo
# --------------------------------------------------------------------------- #

def groups_of(claims):
    """Grupos del token. HTTP API los envía como texto: '[administrador miembro]'."""
    raw = claims.get("cognito:groups", "")
    if isinstance(raw, list):
        return {str(g).strip() for g in raw}
    return {g.strip() for g in re.split(r"[,\s]+", str(raw).strip().strip("[]")) if g.strip()}


def require_group(group):
    """Middleware que solo deja pasar a usuarios del grupo indicado.

    El token ya viene validado por el autorizador JWT; aquí solo se leen los claims.
    Deja el correo del usuario en app.context["email"] para el handler.
    """

    def middleware(app: APIGatewayHttpResolver, next_middleware: NextMiddleware) -> Response:
        claims = app.current_event.request_context.authorizer.jwt_claim or {}
        email = str(claims.get("email") or "").lower()

        if group not in groups_of(claims):
            logger.warning(
                "Acceso denegado",
                extra={"usuario": mask(email), "grupo_requerido": group, "grupos": claims.get("cognito:groups")},
            )
            raise ForbiddenError("No tienes permiso para esta acción.")

        app.append_context(email=email)
        logger.append_keys(usuario=mask(email))
        return next_middleware(app)

    return middleware


# --------------------------------------------------------------------------- #
# Rutas
# --------------------------------------------------------------------------- #

def clean_text(value):
    """Texto plano normalizado: NFC, sin caracteres de control ni espacios repetidos."""
    text = unicodedata.normalize("NFC", str(value or ""))
    text = CONTROL_CHARS.sub("", text).replace("\r\n", "\n")
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{3,}", "\n\n", text)  # máximo una línea en blanco seguida
    return text.strip()


def take_daily_slot(email, kind="SUGGESTIONS", label="sugerencias"):
    """Suma 1 al contador del día (por tipo); 429 si ya se llegó al límite."""
    today = datetime.now(MX_TZ)
    expires = int((today + timedelta(days=2)).timestamp())
    try:
        table.update_item(
            Key={"PK": f"USER#{email}", "SK": f"{kind}#{today.strftime('%Y-%m-%d')}"},
            UpdateExpression="SET sent = if_not_exists(sent, :zero) + :one, #ttl = :ttl",
            ConditionExpression="attribute_not_exists(sent) OR sent < :limit",
            ExpressionAttributeNames={"#ttl": "ttl"},  # "ttl" es palabra reservada de DynamoDB
            ExpressionAttributeValues={":zero": 0, ":one": 1, ":limit": DAILY_LIMIT, ":ttl": expires},
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise TooManyRequestsError(f"Puedes enviar hasta {DAILY_LIMIT} {label} al día. Intenta mañana.")
        raise


def student_email(action):
    """Correo del miembro autenticado; solo estudiantes pueden usar el buzón."""
    email = app.context["email"]
    if not STUDENT_EMAIL.match(email):
        raise ForbiddenError(f"Solo los estudiantes pueden enviar {action}.")
    return email


def safe_text(value, field, lo, hi):
    """Texto plano validado: longitud y sin HTML ni scripts."""
    text = clean_text(value)
    if not lo <= len(text) <= hi:
        raise BadRequestError(f"{field} debe tener entre {lo} y {hi} caracteres.")
    if UNSAFE_TEXT.search(text):
        logger.warning("Texto rechazado por contenido HTML o script", extra={"campo": field})
        raise BadRequestError(f"{field} no puede contener código HTML ni scripts.")
    return text


@app.post("/suggestions", middlewares=[require_group(MEMBER_GROUP)])
def send_suggestion():
    """Body: {text, anonymous}  ->  201 {id}"""
    email = student_email("sugerencias")
    data = body()
    text = safe_text(data.get("text"), "La sugerencia", TEXT_MIN, TEXT_MAX)
    anonymous = data.get("anonymous", False)
    if not isinstance(anonymous, bool):
        raise BadRequestError("anonymous debe ser true o false.")

    take_daily_slot(email)

    suggestion_id = str(uuid.uuid4())
    created_at = now_iso()
    item = {
        "PK": f"SUGGESTION#{suggestion_id}",
        "SK": "META",
        "GSI1PK": PARTITION,
        "GSI1SK": created_at,  # más recientes primero en el panel
        "id": suggestion_id,
        "text": text,
        "date": created_at,
    }
    if not anonymous:
        item["email"] = email

    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    logger.info("Sugerencia recibida", extra={"sugerencia": suggestion_id, "anonima": anonymous})
    return {"id": suggestion_id}, 201


@app.get("/admin/suggestions", middlewares=[require_group(ADMIN_GROUP)])
def list_suggestions():
    """Sugerencias de la más reciente a la más antigua.

    Query: limit=1..50 (20), cursor=<nextCursor anterior>
    Respuesta: {items: [{id, text, date, email?}], nextCursor}
    """
    items, next_cursor = page_query(PARTITION)
    return {
        "items": clean([pick(i, ("id", "text", "date", "email")) for i in items]),
        "nextCursor": next_cursor,
    }


# --------------------------------------------------------------------------- #
# Mensajes al líder
# --------------------------------------------------------------------------- #

def forward_to_leader(message):
    """Reenvía el mensaje al correo del líder; responder desde el correo le llega al estudiante."""
    e = html.escape
    when = datetime.strptime(message["date"], ISO_FORMAT).replace(tzinfo=timezone.utc).astimezone(MX_TZ)
    sent = when.strftime("%d/%m/%Y %H:%M")
    text = (
        f"Mensaje de {message['email']} ({sent}, hora del centro de México)\n\n"
        f"Asunto: {message['subject']}\n\n{message['text']}\n\n"
        "Responde a este correo para contestarle al estudiante."
    )
    body_html = (
        '<div style="font-family:Arial,sans-serif;color:#16213A;max-width:560px">'
        f'<p style="color:#4B5563;margin:0 0 12px">Mensaje de <b>{e(message["email"])}</b> · {e(sent)}</p>'
        f'<h2 style="font-size:18px;margin:0 0 12px">{e(message["subject"])}</h2>'
        f'<p style="white-space:pre-wrap;line-height:1.5;margin:0 0 20px">{e(message["text"])}</p>'
        '<p style="color:#6B7280;font-size:12px;margin:0">Responde a este correo para contestarle al estudiante.</p>'
        "</div>"
    )
    ses.send_email(
        FromEmailAddress=SENDER_EMAIL,
        Destination={"ToAddresses": [LEADER_EMAIL]},
        ReplyToAddresses=[message["email"]],
        Content={"Simple": {
            "Subject": {"Data": f"[Builder Group] {message['subject']}", "Charset": "UTF-8"},
            "Body": {"Text": {"Data": text, "Charset": "UTF-8"}, "Html": {"Data": body_html, "Charset": "UTF-8"}},
        }},
    )


@app.post("/messages", middlewares=[require_group(MEMBER_GROUP)])
def send_message():
    """Body: {subject, text}  ->  201 {id}"""
    email = student_email("mensajes")
    data = body()
    subject = safe_text(data.get("subject"), "El asunto", SUBJECT_MIN, SUBJECT_MAX)
    if "\n" in subject:
        raise BadRequestError("El asunto debe ir en una sola línea.")
    text = safe_text(data.get("text"), "El mensaje", MESSAGE_MIN, MESSAGE_MAX)

    take_daily_slot(email, "MESSAGES", "mensajes")

    message_id = str(uuid.uuid4())
    created_at = now_iso()
    item = {
        "PK": f"MESSAGE#{message_id}",
        "SK": "META",
        "GSI1PK": MESSAGES_PARTITION,
        "GSI1SK": created_at,
        "id": message_id,
        "email": email,
        "subject": subject,
        "text": text,
        "date": created_at,
    }
    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    logger.info("Mensaje recibido", extra={"mensaje": message_id})

    try:
        forward_to_leader(item)
    except ClientError:
        logger.exception("No se pudo reenviar el mensaje al líder; queda disponible en el panel")

    return {"id": message_id}, 201


@app.get("/admin/messages", middlewares=[require_group(ADMIN_GROUP)])
def list_messages():
    """Mensajes del más reciente al más antiguo.

    Query: limit=1..50 (20), cursor=<nextCursor anterior>
    Respuesta: {items: [{id, email, subject, text, date}], nextCursor}
    """
    items, next_cursor = page_query(MESSAGES_PARTITION)
    return {
        "items": clean([pick(i, ("id", "email", "subject", "text", "date")) for i in items]),
        "nextCursor": next_cursor,
    }


# --------------------------------------------------------------------------- #
# Errores
# --------------------------------------------------------------------------- #

def json_response(status, message):
    return Response(
        status_code=status,
        content_type=content_types.APPLICATION_JSON,
        body=json.dumps({"message": message}, ensure_ascii=False),
    )


@app.not_found
def not_found(ex: NotFoundError):
    # Se usa tanto para rutas inexistentes como para NotFoundError lanzados en los handlers
    msg = getattr(ex, "msg", "") or ""
    if msg in ("", "Not found"):
        msg = "Ruta no encontrada."
    return json_response(404, msg)


@app.exception_handler(Exception)
def unexpected_error(ex: Exception):
    # Los errores HTTP conservan su código; sin esto terminarían como 500
    if isinstance(ex, ServiceError):
        return json_response(ex.status_code, ex.msg)
    logger.exception("Error no controlado")
    return json_response(500, "Ocurrió un error. Intenta de nuevo.")


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

@logger.inject_lambda_context(correlation_id_path=correlation_paths.API_GATEWAY_HTTP)
def lambda_handler(event, context):
    return app.resolve(event, context)
