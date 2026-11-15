"""
Eventos - Student Builder Group rewards platform

Lambda detrás de API Gateway (HTTP API, payload 2.0) para publicar y consultar eventos.

Rutas:
    GET   /events                  pública         próximos eventos (desde ahora, en orden de fecha)
    GET   /events/<event_id>       pública         detalle de un evento
    GET   /me/subscription         miembro         ¿estoy suscrito a los avisos de eventos?
    PUT   /me/subscription         miembro         suscribirse o desuscribirse
    GET   /admin/events            administrador   eventos paginados: ?status=active|deleted&limit=&cursor=
    POST  /admin/events            administrador   crear evento
    PATCH  /admin/events/<event_id> administrador  editar cualquier campo o restaurar un evento eliminado
    DELETE /admin/events/<event_id> administrador  eliminar evento (borrado lógico)
    GET   /admin/events/<event_id>/attendance  administrador  estudiantes registrados en el evento
    POST  /admin/events/<event_id>/attendance  administrador  registrar asistencia y sumar puntos

Asistencia: por cada estudiante, una transacción escribe el movimiento en su historial
(USER#<correo> / ACT#<fecha del evento>#<event_id>, con GSI1PK = EVENT#<event_id>)
y suma los puntos del evento a su perfil. El movimiento solo se crea si no existe, así
que registrar dos veces al mismo estudiante no suma dos veces; y solo se registra a
quien ya tiene perfil (cuenta confirmada).

Borrado lógico: los movimientos de asistencia guardan el id del evento
(SK ACT#<fecha>#<event_id> y GSI1PK EVENT#<event_id>), así que un evento nunca se
borra de la tabla. Al eliminarlo se marca deleted=true y su GSI1PK cambia de
EVENTS a EVENTS#DELETED: sale de todas las consultas de eventos sin filtros y
sigue disponible en DynamoDB para auditoría o para restaurarlo.

Suscripción: se guarda en el perfil del alumno (USER#<correo> / PROFILE). Los suscritos
además llevan GSI1PK = SUBSCRIBERS (índice disperso), para obtener la lista de
destinatarios con una sola consulta cuando se envíen los avisos.

La autenticación la resuelve el autorizador JWT de la HTTP API. Aquí solo se valida
el grupo de Cognito de cada ruta. CORS se configura en la HTTP API.

Requiere la capa AWSLambdaPowertoolsPythonV3-python313-<arquitectura>.

Variables de entorno (todas obligatorias):
    TABLE_NAME   tabla de DynamoDB
"""

import base64
import json
import os
import re
import secrets
import unicodedata
from datetime import datetime, timezone
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
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]

ADMIN_GROUP = "administrador"
MEMBER_GROUP = "miembro"

# Correo institucional con número de control antes de la @
INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()
STUDENT_EMAIL = re.compile(r"^(?=[^@]*\d)[a-z0-9._-]+@" + re.escape(INSTITUTIONAL_DOMAIN) + "$")
EVENT_ID_RE = re.compile(r"^[a-z0-9-]{1,80}$")

# Estudiantes por solicitud de registro de asistencia (cada uno es una transacción)
ATTENDANCE_MAX = 200

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"  # UTC; ordenable como texto en GSI1SK

# Campos de un evento que se exponen fuera de la Lambda
PUBLIC_FIELDS = ("id", "title", "date", "location", "points", "description")
ADMIN_FIELDS = (*PUBLIC_FIELDS, "deleted", "deletedAt")

# Particiones del índice GSI1 (ordenadas por fecha del evento)
EVENTS_ACTIVE = "EVENTS"
EVENTS_DELETED = "EVENTS#DELETED"
STATUS_PARTITIONS = {"active": EVENTS_ACTIVE, "deleted": EVENTS_DELETED}

PAGE_SIZE_MAX = 50

logger = Logger(service="eventos")

# strip_prefixes permite servir la API detrás de CloudFront en /api/*
app = APIGatewayHttpResolver(strip_prefixes=["/api"])

table = boto3.resource("dynamodb").Table(TABLE_NAME)
ddb = boto3.client("dynamodb")  # transact_write_items solo existe en el client
serializer = TypeSerializer()


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


def as_int(value, field, lo, hi):
    """Valida un entero dentro de [lo, hi]. Rechaza bool, texto y decimales."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or int(value) != value:
        raise BadRequestError(f"{field} debe ser un número entero.")
    value = int(value)
    if not lo <= value <= hi:
        raise BadRequestError(f"{field} debe estar entre {lo} y {hi}.")
    return value


def slugify(text, max_len=40):
    """'Taller: tu primera API con Lambda' -> 'taller-tu-primera-api-con-lambda'"""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text[:max_len].strip("-") or "evento"


def query_all(**kwargs):
    """Query paginado: DynamoDB regresa máximo 1 MB por llamada."""
    items = []
    while True:
        page = table.query(**kwargs)
        items.extend(page["Items"])
        if "LastEvaluatedKey" not in page:
            return items
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


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
# Validación
# --------------------------------------------------------------------------- #

def validate_title(value):
    title = str(value or "").strip()
    if not 3 <= len(title) <= 120:
        raise BadRequestError("El título debe tener entre 3 y 120 caracteres.")
    return title


def validate_location(value):
    location = str(value or "").strip()
    if not 2 <= len(location) <= 120:
        raise BadRequestError("El lugar debe tener entre 2 y 120 caracteres.")
    return location


def validate_description(value):
    description = str(value or "").strip()
    if len(description) > 400:
        raise BadRequestError("La descripción no puede pasar de 400 caracteres.")
    return description


def validate_date(value):
    """Fecha ISO 8601 con zona horaria; se guarda normalizada en UTC.

    Se aceptan fechas pasadas para poder registrar eventos que ya ocurrieron
    (y después su asistencia); solo los futuros aparecen en GET /events.
    """
    try:
        date = datetime.fromisoformat(str(value))
    except (TypeError, ValueError):
        raise BadRequestError("La fecha no es válida. Usa formato ISO 8601. Ej.: 2026-10-20T22:00:00Z")
    if date.tzinfo is None:
        raise BadRequestError("La fecha debe incluir zona horaria. Ej.: 2026-10-20T22:00:00Z")
    return date.astimezone(timezone.utc).strftime(ISO_FORMAT)


# --------------------------------------------------------------------------- #
# Rutas públicas
# --------------------------------------------------------------------------- #

@app.get("/events")
def upcoming_events():
    """Eventos desde este momento, del más próximo al más lejano."""
    items = query_all(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(EVENTS_ACTIVE) & Key("GSI1SK").gte(now_iso()),
    )
    return clean([pick(i, PUBLIC_FIELDS) for i in items])


@app.get("/events/<event_id>")
def get_event(event_id: str):
    """Detalle de un evento (próximo o pasado)."""
    if not EVENT_ID_RE.match(event_id):
        raise BadRequestError("Identificador de evento no válido.")
    item = table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": "META"}).get("Item")
    if not item or item.get("deleted") is True:
        raise NotFoundError("El evento no existe.")
    return clean(pick(item, PUBLIC_FIELDS))


# --------------------------------------------------------------------------- #
# Suscripción a avisos (miembros)
# --------------------------------------------------------------------------- #

def student_email():
    """Correo del miembro autenticado; solo alumnos pueden suscribirse."""
    email = app.context["email"]
    if not STUDENT_EMAIL.match(email):
        raise ForbiddenError("Solo los estudiantes pueden suscribirse a los avisos.")
    return email


@app.get("/me/subscription", middlewares=[require_group(MEMBER_GROUP)])
def get_subscription():
    """Respuesta: {subscribed}"""
    email = student_email()
    profile = table.get_item(Key={"PK": f"USER#{email}", "SK": "PROFILE"}).get("Item") or {}
    return {"subscribed": profile.get("subscribed") is True}


@app.put("/me/subscription", middlewares=[require_group(MEMBER_GROUP)])
def set_subscription():
    """Body: {subscribed: true|false}  ->  {subscribed}

    Suscrito:      subscribed=true y GSI1PK/GSI1SK = SUBSCRIBERS/<correo>
    No suscrito:   subscribed=false y se quitan GSI1PK/GSI1SK (sale del índice)
    Si el perfil aún no existe se crea con 0 puntos, sin pisar datos existentes.
    """
    email = student_email()
    data = body()
    subscribed = data.get("subscribed")
    if not isinstance(subscribed, bool):
        raise BadRequestError("subscribed debe ser true o false.")

    now = now_iso()
    sets = [
        "subscribed = :sub",
        "subscriptionUpdatedAt = :now",
        "email = :email",
        "points = if_not_exists(points, :zero)",
        "createdAt = if_not_exists(createdAt, :now)",
    ]
    values = {":sub": subscribed, ":now": now, ":email": email, ":zero": 0}

    if subscribed:
        sets += ["GSI1PK = :gpk", "GSI1SK = :email"]
        values[":gpk"] = "SUBSCRIBERS"
        expression = "SET " + ", ".join(sets)
    else:
        expression = "SET " + ", ".join(sets) + " REMOVE GSI1PK, GSI1SK"

    table.update_item(
        Key={"PK": f"USER#{email}", "SK": "PROFILE"},
        UpdateExpression=expression,
        ExpressionAttributeValues=values,
    )
    logger.info("Suscripción actualizada", extra={"suscrito": subscribed})
    return {"subscribed": subscribed}


# --------------------------------------------------------------------------- #
# Administración
# --------------------------------------------------------------------------- #

@app.get("/admin/events", middlewares=[require_group(ADMIN_GROUP)])
def list_events():
    """Eventos paginados por fecha del evento, del más reciente al más antiguo.

    Query: status=active|deleted (obligatorio), limit=1..50 (20), cursor=<nextCursor anterior>
    Respuesta: {items, nextCursor}  (nextCursor = null cuando ya no hay más)
    """
    status = app.current_event.get_query_string_value("status", "")
    if status not in STATUS_PARTITIONS:
        raise BadRequestError("status debe ser active o deleted.")
    items, next_cursor = page_query(STATUS_PARTITIONS[status])
    return {"items": clean([pick(i, ADMIN_FIELDS) for i in items]), "nextCursor": next_cursor}


@app.post("/admin/events", middlewares=[require_group(ADMIN_GROUP)])
def create_event():
    """Crea un evento.

    Body: {title, date, location, points, description?}
        date: ISO 8601 con zona horaria (la SPA la envía en UTC)
    """
    data = body()
    title = validate_title(data.get("title"))
    date = validate_date(data.get("date"))
    location = validate_location(data.get("location"))
    points = as_int(data.get("points"), "points", 0, 10_000)
    description = validate_description(data.get("description"))

    # La fecha en el id ayuda a distinguir ediciones del mismo taller en la consola
    event_id = f"{slugify(title)}-{date[:10].replace('-', '')}-{secrets.token_hex(2)}"
    item = {
        "PK": f"EVENT#{event_id}",
        "SK": "META",
        "GSI1PK": EVENTS_ACTIVE,
        "GSI1SK": date,  # ordena por fecha y permite filtrar los próximos
        "id": event_id,
        "title": title,
        "date": date,
        "location": location,
        "points": points,
        "createdAt": now_iso(),
        "createdBy": app.context["email"],
    }
    if description:
        item["description"] = description

    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    logger.info("Evento creado", extra={"evento": event_id, "fecha": date, "puntos": points})

    return clean(pick(item, PUBLIC_FIELDS)), 201


@app.patch("/admin/events/<event_id>", middlewares=[require_group(ADMIN_GROUP)])
def update_event(event_id: str):
    """Edita cualquier campo de un evento. Solo se cambian los campos enviados.

    Body: {title?, date?, location?, points?, description?, deleted?}
        description: "" la quita
        deleted:     false restaura un evento eliminado; true lo elimina (igual que DELETE)

    La asistencia ya registrada no cambia: cada movimiento guarda su propia copia
    del título y los puntos.
    """
    if not EVENT_ID_RE.match(event_id):
        raise BadRequestError("Identificador de evento no válido.")
    data = body()
    key = {"PK": f"EVENT#{event_id}", "SK": "META"}

    current = table.get_item(Key=key, ConsistentRead=True).get("Item")
    if not current:
        raise NotFoundError("El evento no existe.")

    # Se usan alias (#x) porque "date" y otros nombres son palabras reservadas de DynamoDB
    sets, removes = {}, []

    if "title" in data:
        sets["title"] = validate_title(data["title"])
    if "date" in data:
        sets["date"] = validate_date(data["date"])
    if "location" in data:
        sets["location"] = validate_location(data["location"])
    if "points" in data:
        sets["points"] = as_int(data["points"], "points", 0, 10_000)
    if "description" in data:
        description = validate_description(data["description"])
        if description:
            sets["description"] = description
        else:
            removes.append("description")

    was_deleted = current.get("deleted") is True
    deleted = was_deleted
    if "deleted" in data:
        if not isinstance(data["deleted"], bool):
            raise BadRequestError("deleted debe ser true o false.")
        deleted = data["deleted"]
        if deleted and not was_deleted:
            sets.update({"deleted": True, "deletedAt": now_iso(), "deletedBy": app.context["email"]})
        elif not deleted and was_deleted:
            sets["deleted"] = False
            removes += ["deletedAt", "deletedBy"]

    if not sets and not removes:
        raise BadRequestError("No hay cambios que guardar.")

    # Activo o eliminado decide la partición; la fecha ordena el índice
    sets["GSI1PK"] = EVENTS_DELETED if deleted else EVENTS_ACTIVE
    sets["GSI1SK"] = sets.get("date", current["date"])
    sets["updatedAt"] = now_iso()
    sets["updatedBy"] = app.context["email"]

    names, values, parts = {}, {}, []
    for i, (attr, value) in enumerate(sets.items()):
        names[f"#s{i}"] = attr
        values[f":s{i}"] = value
        parts.append(f"#s{i} = :s{i}")
    expression = "SET " + ", ".join(parts)
    if removes:
        for i, attr in enumerate(removes):
            names[f"#r{i}"] = attr
        expression += " REMOVE " + ", ".join(f"#r{i}" for i in range(len(removes)))

    try:
        res = table.update_item(
            Key=key,
            UpdateExpression=expression,
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
            ConditionExpression="attribute_exists(PK)",  # evita crear el evento si no existe
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise NotFoundError("El evento no existe.")
        raise

    logger.info("Evento actualizado", extra={"evento": event_id, "campos": sorted(k for k in data if k in ADMIN_FIELDS)})
    return clean(pick(res["Attributes"], ADMIN_FIELDS))


@app.delete("/admin/events/<event_id>", middlewares=[require_group(ADMIN_GROUP)])
def delete_event(event_id: str):
    """Borrado lógico: marca el evento y lo saca del índice EVENTS."""
    if not EVENT_ID_RE.match(event_id):
        raise BadRequestError("Identificador de evento no válido.")

    try:
        table.update_item(
            Key={"PK": f"EVENT#{event_id}", "SK": "META"},
            UpdateExpression="SET deleted = :true, deletedAt = :now, deletedBy = :by, GSI1PK = :gpk",
            # Solo eventos existentes y que no estén eliminados
            ConditionExpression="attribute_exists(PK) AND (attribute_not_exists(deleted) OR deleted = :false)",
            ExpressionAttributeValues={
                ":true": True,
                ":false": False,
                ":now": now_iso(),
                ":by": app.context["email"],
                ":gpk": EVENTS_DELETED,
            },
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise NotFoundError("El evento no existe o ya fue eliminado.")
        raise

    logger.info("Evento eliminado", extra={"evento": event_id})
    return {"id": event_id, "deleted": True}


# --------------------------------------------------------------------------- #
# Asistencia (suma de puntos)
# --------------------------------------------------------------------------- #

def active_event(event_id):
    """Evento existente y no eliminado; 404 en otro caso."""
    if not EVENT_ID_RE.match(event_id):
        raise BadRequestError("Identificador de evento no válido.")
    item = table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": "META"}, ConsistentRead=True).get("Item")
    if not item or item.get("deleted") is True:
        raise NotFoundError("El evento no existe o fue eliminado.")
    return item


@app.get("/admin/events/<event_id>/attendance", middlewares=[require_group(ADMIN_GROUP)])
def list_attendance(event_id: str):
    """Respuesta: [{email, date}] ordenado por correo; date = cuándo se registró la asistencia."""
    active_event(event_id)
    items = query_all(IndexName="GSI1", KeyConditionExpression=Key("GSI1PK").eq(f"EVENT#{event_id}"))
    return sorted(
        ({"email": i["GSI1SK"].removeprefix("USER#"), "date": i.get("registeredAt") or i.get("date", "")}
         for i in items),
        key=lambda a: a["email"],
    )


def register_one(event, email, admin, now):
    """Registra a un estudiante. Regresa 'added', 'skipped' (ya estaba) o 'notRegistered' (sin cuenta)."""
    points = int(event.get("points", 0))
    activity = {
        "PK": f"USER#{email}",
        "SK": f"ACT#{event['date']}#{event['id']}",  # fecha del evento: ordena el historial y evita duplicados
        "GSI1PK": f"EVENT#{event['id']}",
        "GSI1SK": f"USER#{email}",
        "date": event["date"],
        "title": event["title"],
        "points": points,
        "registeredAt": now,
        "registeredBy": admin,
    }
    try:
        ddb.transact_write_items(TransactItems=[
            # 0. Movimiento en el historial, solo si no estaba registrado
            {"Put": {
                "TableName": TABLE_NAME,
                "Item": {k: serializer.serialize(v) for k, v in activity.items()},
                "ConditionExpression": "attribute_not_exists(PK)",
            }},
            # 1. Sumar puntos, solo si el estudiante ya tiene perfil
            {"Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"USER#{email}"}, "SK": {"S": "PROFILE"}},
                "UpdateExpression": "SET points = if_not_exists(points, :zero) + :p",
                "ConditionExpression": "attribute_exists(PK)",
                "ExpressionAttributeValues": {":zero": {"N": "0"}, ":p": {"N": str(points)}},
            }},
        ])
        return "added"
    except ClientError as e:
        if e.response["Error"]["Code"] != "TransactionCanceledException":
            raise
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if reasons and reasons[0] == "ConditionalCheckFailed":
            return "skipped"
        if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
            return "notRegistered"
        raise


@app.post("/admin/events/<event_id>/attendance", middlewares=[require_group(ADMIN_GROUP)])
def register_attendance(event_id: str):
    """Body: {emails: [...]}  ->  {added, skipped, notRegistered, invalid}

    added          se registraron y recibieron los puntos del evento
    skipped        ya estaban registrados en el evento (no se suman puntos otra vez)
    notRegistered  no tienen cuenta en la plataforma
    invalid        no son correos de estudiante
    """
    event = active_event(event_id)
    data = body()
    raw = data.get("emails")
    if not isinstance(raw, list) or not raw:
        raise BadRequestError("Envía la lista de correos en emails.")

    emails = list(dict.fromkeys(str(e).strip().lower() for e in raw if str(e).strip()))  # sin repetidos, en orden
    if len(emails) > ATTENDANCE_MAX:
        raise BadRequestError(f"Registra máximo {ATTENDANCE_MAX} estudiantes por solicitud.")

    result = {"added": [], "skipped": [], "notRegistered": [], "invalid": []}
    admin, now = app.context["email"], now_iso()
    for email in emails:
        if not STUDENT_EMAIL.match(email):
            result["invalid"].append(email)
            continue
        result[register_one(event, email, admin, now)].append(email)

    logger.info("Asistencia registrada", extra={"evento": event_id, **{k: len(v) for k, v in result.items()}})
    return result


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
