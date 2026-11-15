"""
Canjes - Student Builder Group rewards platform

Lambda detrás de API Gateway (HTTP API, payload 2.0) para canjear recompensas,
consultar canjes, verificar vales y administrar las solicitudes de canje.

Rutas:
    POST  /redemptions                     miembro         canjear una recompensa
    GET   /me/redemptions                  miembro         mis canjes, paginado: ?limit=&cursor=
    GET   /coupons/<coupon_id>             pública         verificar un vale institucional (docentes)
    GET   /admin/redemptions               administrador   canjes por estado: ?status=&limit=&cursor=
    PATCH /admin/redemptions/<redemption_id>  administrador  cambiar el estado de un canje

Estados de un canje:
    AWS            PENDIENTE  -> ENTREGADO | REEMBOLSADO
    INSTITUCIONAL  VIGENTE    -> APLICADO  | REEMBOLSADO
Un canje no se cancela: si hace falta (por ejemplo, lo pide un docente) se reembolsa.
El reembolso devuelve los puntos al estudiante, registra el movimiento en su historial,
regresa la unidad al stock y, en los institucionales, invalida el vale.
ENTREGADO, APLICADO y REEMBOLSADO son estados finales.

El saldo nunca se toma de la SPA: se lee del perfil en DynamoDB y además la
transacción del canje exige points >= costo, por si otro canje ocurre al mismo tiempo.

La autenticación la resuelve el autorizador JWT de la HTTP API. Aquí solo se valida
el grupo de Cognito de cada ruta. CORS se configura en la HTTP API.

Requiere la capa AWSLambdaPowertoolsPythonV3-python313-<arquitectura>.

Variables de entorno (todas obligatorias):
    TABLE_NAME    tabla de DynamoDB
    LEADER_NAME   nombre que aparece como "Expide" en los vales
"""

import base64
import json
import os
import re
import uuid
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
from boto3.dynamodb.conditions import Attr, Key
from boto3.dynamodb.types import TypeSerializer
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]
LEADER_NAME = os.environ["LEADER_NAME"]

ADMIN_GROUP = "administrador"
MEMBER_GROUP = "miembro"

# Correo institucional con número de control antes de la @
INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()
STUDENT_EMAIL = re.compile(r"^(?=[^@]*\d)[a-z0-9._-]+@" + re.escape(INSTITUTIONAL_DOMAIN) + "$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$")
REWARD_ID_RE = re.compile(r"^[a-z0-9-]{1,60}$")

# Actividades más recientes que se copian al vale
VOUCHER_ACTIVITIES = 10

# Estado actual -> estados a los que puede pasar
TRANSITIONS = {
    "PENDIENTE": {"ENTREGADO", "REEMBOLSADO"},
    "VIGENTE": {"APLICADO", "REEMBOLSADO"},
}
STATUSES = ("PENDIENTE", "VIGENTE", "ENTREGADO", "APLICADO", "REEMBOLSADO")

MEMBER_FIELDS = ("id", "rewardId", "rewardName", "rewardDescription", "type", "points", "date", "status", "contact")
ADMIN_FIELDS = (*MEMBER_FIELDS, "email", "refundedAt")
COUPON_FIELDS = ("id", "email", "rewardName", "rewardDescription", "points", "date", "status", "issuedBy", "activities")

PAGE_SIZE_MAX = 50
USER_PAGE_MAX = 50

logger = Logger(service="canjes")

# strip_prefixes permite servir la API detrás de CloudFront en /api/*
app = APIGatewayHttpResolver(strip_prefixes=["/api"])

# Clientes fuera del handler para reutilizarlos entre invocaciones
table = boto3.resource("dynamodb").Table(TABLE_NAME)
ddb = boto3.client("dynamodb")  # transact_write_items solo existe en el client
serializer = TypeSerializer()


class ConflictError(ServiceError):
    """409: la petición es válida pero choca con el estado actual (sin stock, sin puntos, duplicado)."""

    def __init__(self, msg):
        super().__init__(409, msg)


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
    """Fecha UTC en ISO 8601; ordenable como texto en SK y GSI1SK."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


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
# Canje y consultas del miembro
# --------------------------------------------------------------------------- #

def validate_contact(raw):
    """Datos de entrega para recompensas AWS."""
    if not isinstance(raw, dict):
        raise BadRequestError("Faltan tus datos de contacto para la entrega.")

    full_name = re.sub(r"\s+", " ", str(raw.get("fullName", ""))).strip()
    if not 5 <= len(full_name) <= 120:
        raise BadRequestError("Escribe tu nombre completo.")

    phone = re.sub(r"\D", "", str(raw.get("phone", "")))  # acepta espacios y guiones
    if len(phone) != 10:
        raise BadRequestError("El celular debe tener 10 dígitos.")

    semester = as_int(raw.get("semester"), "Semestre", 1, 8)

    group = str(raw.get("group", "")).upper()
    if group not in ("A", "B"):
        raise BadRequestError("El grupo debe ser A o B.")

    return {"fullName": full_name, "phone": phone, "semester": semester, "group": group}


def is_earned(item):
    """Actividad realizada por el estudiante: puntos positivos que no son un reembolso.

    Los reembolsos también suman puntos, pero no son una actividad. Se reconocen por
    kind = "refund" o, en los registrados antes de ese atributo, por su SK (...#refund-<id>).
    """
    if int(item.get("points", 0)) <= 0:
        return False
    return item.get("kind") != "refund" and not item["SK"].rsplit("#", 1)[-1].startswith("refund-")


def earned_activities(email):
    """Las VOUCHER_ACTIVITIES actividades más recientes del estudiante (sin canjes ni reembolsos).

    El SK ACT#<fecha> ya ordena por fecha, así que basta con leer en orden descendente
    y detenerse al juntar las necesarias.
    """
    acts = []
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(f"USER#{email}") & Key("SK").begins_with("ACT#"),
        "ScanIndexForward": False,
        "ConsistentRead": True,
        "Limit": 25,  # páginas chicas: normalmente basta una lectura
    }
    while len(acts) < VOUCHER_ACTIVITIES:
        page = table.query(**kwargs)
        acts.extend(
            {"date": i.get("date", ""), "title": i.get("title", ""), "points": int(i["points"])}
            for i in page["Items"]
            if is_earned(i)
        )
        if "LastEvaluatedKey" not in page:
            break
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]
    return acts[:VOUCHER_ACTIVITIES]


def put_new(item):
    """Put para la transacción que falla si el item ya existe."""
    return {
        "Put": {
            "TableName": TABLE_NAME,
            "Item": {k: serializer.serialize(v) for k, v in item.items()},
            "ConditionExpression": "attribute_not_exists(PK)",
        }
    }


@app.post("/redemptions", middlewares=[require_group(MEMBER_GROUP)])
def redeem():
    """Canjea una recompensa.

    Body: {rewardId, requestId, contact?}  (contact solo para recompensas AWS)
    Respuesta 201: {redemption, points, stock}
    """
    email = app.context["email"]
    if not STUDENT_EMAIL.match(email):
        raise ForbiddenError("Solo los estudiantes pueden canjear recompensas.")

    data = body()

    reward_id = str(data.get("rewardId", ""))
    if not REWARD_ID_RE.match(reward_id):
        raise BadRequestError("Recompensa no válida.")

    # La SPA genera un requestId por intento de canje para evitar cobros dobles
    request_id = str(data.get("requestId") or "").lower()
    if not UUID_RE.match(request_id):
        raise BadRequestError("Falta el identificador de la solicitud. Recarga la página.")

    # Revisión previa para responder con mensajes claros; la garantía está en la transacción
    reward = table.get_item(Key={"PK": f"REWARD#{reward_id}", "SK": "META"}, ConsistentRead=True).get("Item")
    if not reward or reward.get("active") is not True:
        raise NotFoundError("Esta recompensa ya no está disponible.")
    if int(reward.get("stock", 0)) <= 0:
        raise ConflictError("Esta recompensa se agotó.")

    rtype = reward["type"]
    cost = int(reward["points"])

    # El saldo se lee de DynamoDB; lo que muestre la SPA no cuenta
    profile = table.get_item(Key={"PK": f"USER#{email}", "SK": "PROFILE"}, ConsistentRead=True).get("Item") or {}
    balance = int(profile.get("points", 0))
    if balance < cost:
        raise ConflictError(f"No tienes puntos suficientes: tienes {balance} y necesitas {cost}.")

    contact = validate_contact(data.get("contact")) if rtype == "AWS" else None

    rid = str(uuid.uuid4())  # id del canje y folio del vale
    date = now_iso()
    status = "PENDIENTE" if rtype == "AWS" else "VIGENTE"

    # Copia de los datos de la recompensa al momento del canje
    redemption = {
        "id": rid,
        "rewardId": reward_id,
        "rewardName": reward["name"],
        "type": rtype,
        "points": cost,
        "date": date,
        "status": status,
        "email": email,
    }
    if reward.get("description"):
        redemption["rewardDescription"] = reward["description"]
    if contact:
        redemption["contact"] = contact

    items = [
        # 0. Stock: hay existencias, sigue visible y el precio no cambió
        {
            "Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"REWARD#{reward_id}"}, "SK": {"S": "META"}},
                "UpdateExpression": "SET stock = stock - :one",
                "ConditionExpression": "stock > :zero AND active = :true AND points = :cost",
                "ExpressionAttributeValues": {
                    ":one": {"N": "1"},
                    ":zero": {"N": "0"},
                    ":true": {"BOOL": True},
                    ":cost": {"N": str(cost)},
                },
            }
        },
        # 1. Saldo suficiente (de nuevo, por si otro canje ocurrió al mismo tiempo)
        {
            "Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"USER#{email}"}, "SK": {"S": "PROFILE"}},
                "UpdateExpression": "SET points = points - :cost",
                "ConditionExpression": "points >= :cost",
                "ExpressionAttributeValues": {":cost": {"N": str(cost)}},
            }
        },
        # 2. Registro del canje (GSI1 agrupa por estado para el panel)
        put_new({
            "PK": f"USER#{email}",
            "SK": f"RDM#{date}#{rid}",
            "GSI1PK": f"RDM#{status}",
            "GSI1SK": date,
            **redemption,
        }),
        # 3. Movimiento negativo en el historial del alumno
        put_new({
            "PK": f"USER#{email}",
            "SK": f"ACT#{date}#{rid}",
            "date": date,
            "title": f"Canje: {reward['name']}",
            "points": -cost,
        }),
    ]

    activities = None
    if rtype == "INSTITUCIONAL":
        activities = earned_activities(email)
        # 4. Cupón que consultan los docentes por folio
        items.append(put_new({
            "PK": f"COUPON#{rid}",
            "SK": "META",
            **redemption,
            "issuedBy": LEADER_NAME,
            "activities": activities,
        }))

    # Último paso: candado del requestId. Si ya existe, se cancela toda la transacción.
    # El ttl permite que DynamoDB lo elimine después de un día.
    dedup_index = len(items)
    items.append(put_new({
        "PK": f"USER#{email}",
        "SK": f"REQ#{request_id}",
        "redemptionId": rid,
        "ttl": int(datetime.now(timezone.utc).timestamp()) + 86400,
    }))

    try:
        ddb.transact_write_items(TransactItems=items, ClientRequestToken=request_id)
    except ClientError as e:
        code = e.response["Error"]["Code"]
        if code == "IdempotentParameterMismatchException":
            raise ConflictError("Este canje ya se registró. Revísalo en Mis canjes.")
        if code != "TransactionCanceledException":
            raise

        # Una razón por paso, en el mismo orden de `items`
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if len(reasons) > dedup_index and reasons[dedup_index] == "ConditionalCheckFailed":
            raise ConflictError("Este canje ya se registró. Revísalo en Mis canjes.")
        if reasons and reasons[0] == "ConditionalCheckFailed":
            raise ConflictError("Esta recompensa se agotó o cambió. Recarga la página.")
        if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
            raise ConflictError("No tienes puntos suficientes.")
        if "TransactionConflict" in reasons:
            raise ConflictError("Hubo mucha demanda en este momento. Intenta de nuevo.")

        logger.error("Canje cancelado", extra={"razones": reasons})
        raise ConflictError("No se pudo completar el canje.")

    # Saldo y stock actualizados para refrescar la SPA sin recargar
    profile = table.get_item(Key={"PK": f"USER#{email}", "SK": "PROFILE"}, ConsistentRead=True).get("Item", {})
    fresh = table.get_item(Key={"PK": f"REWARD#{reward_id}", "SK": "META"}, ConsistentRead=True).get("Item", {})

    logger.info("Canje registrado", extra={"canje": rid, "recompensa": reward_id, "tipo": rtype, "puntos": cost})

    out = dict(redemption)
    if activities is not None:
        out.update({"issuedBy": LEADER_NAME, "activities": activities})  # la SPA muestra el vale de inmediato

    return clean({"redemption": out, "points": profile.get("points", 0), "stock": fresh.get("stock", 0)}), 201


def user_page(email, prefix):
    """Una página de los items del estudiante cuyo SK empieza con `prefix`, más recientes primero.

    Query: limit=1..50 (20), cursor=<nextCursor anterior>
    El cursor solo se acepta si pertenece al mismo estudiante y al mismo tipo de item:
    nadie puede usarlo para leer datos de otra persona.
    """
    ev = app.current_event
    try:
        limit = int(ev.get_query_string_value("limit", "20"))
    except ValueError:
        raise BadRequestError("limit debe ser un número entero.")
    if not 1 <= limit <= USER_PAGE_MAX:
        raise BadRequestError(f"limit debe estar entre 1 y {USER_PAGE_MAX}.")

    pk = f"USER#{email}"
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(pk) & Key("SK").begins_with(prefix),
        "ScanIndexForward": False,  # el SK lleva la fecha: descendente = más recientes primero
        "Limit": limit,
    }
    cursor = ev.get_query_string_value("cursor", "")
    if cursor:
        try:
            key = json.loads(base64.urlsafe_b64decode(cursor + "=" * (-len(cursor) % 4)))
        except (ValueError, json.JSONDecodeError):
            raise BadRequestError("El cursor no es válido.")
        if (not isinstance(key, dict) or set(key) != {"PK", "SK"} or key.get("PK") != pk
                or not str(key.get("SK", "")).startswith(prefix)):
            raise BadRequestError("El cursor no es válido.")
        kwargs["ExclusiveStartKey"] = key

    res = table.query(**kwargs)
    last = res.get("LastEvaluatedKey")
    next_cursor = (
        base64.urlsafe_b64encode(json.dumps(last, separators=(",", ":")).encode()).decode().rstrip("=")
        if last else None
    )
    return res["Items"], next_cursor


@app.get("/me/redemptions", middlewares=[require_group(MEMBER_GROUP)])
def my_redemptions():
    """Canjes del estudiante, del más reciente al más antiguo.

    Respuesta: {items, nextCursor}
    """
    items, next_cursor = user_page(app.context["email"], "RDM#")
    return {"items": clean([pick(i, MEMBER_FIELDS) for i in items]), "nextCursor": next_cursor}


# --------------------------------------------------------------------------- #
# Verificación de vales (pública)
# --------------------------------------------------------------------------- #

@app.get("/coupons/<coupon_id>")
def get_coupon(coupon_id: str):
    """Datos del vale tal como se expidió. No incluye datos de contacto."""
    coupon_id = coupon_id.lower()
    if not UUID_RE.match(coupon_id):
        raise BadRequestError("El folio no es válido.")
    item = table.get_item(Key={"PK": f"COUPON#{coupon_id}", "SK": "META"}).get("Item")
    if not item:
        raise NotFoundError("No existe un vale con ese folio.")
    return clean(pick(item, COUPON_FIELDS))


# --------------------------------------------------------------------------- #
# Administración
# --------------------------------------------------------------------------- #

@app.get("/admin/redemptions", middlewares=[require_group(ADMIN_GROUP)])
def list_redemptions():
    """Canjes de todos los estudiantes en un estado, del más reciente al más antiguo.

    Query: status=PENDIENTE|VIGENTE|ENTREGADO|APLICADO|REEMBOLSADO (obligatorio),
           limit=1..50 (20), cursor=<nextCursor anterior>
    Respuesta: {items, nextCursor}
    """
    status = app.current_event.get_query_string_value("status", "")
    if status not in STATUSES:
        raise BadRequestError("status debe ser PENDIENTE, VIGENTE, ENTREGADO, APLICADO o REEMBOLSADO.")
    items, next_cursor = page_query(f"RDM#{status}")
    return {"items": clean([pick(i, ADMIN_FIELDS) for i in items]), "nextCursor": next_cursor}


def find_redemption(email, redemption_id):
    """El SK del canje lleva la fecha (RDM#<fecha>#<id>), así que se busca entre los del estudiante."""
    kwargs = {
        "KeyConditionExpression": Key("PK").eq(f"USER#{email}") & Key("SK").begins_with("RDM#"),
        "FilterExpression": Attr("id").eq(redemption_id),
        "ConsistentRead": True,
    }
    while True:
        page = table.query(**kwargs)
        if page["Items"]:
            return page["Items"][0]
        if "LastEvaluatedKey" not in page:
            return None
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def set_status_item(key, current, new_status, extra_sets=None, gsi=False):
    """Update de transacción que cambia el estado solo si sigue en `current`.

    Se usan alias (#st) porque "status" es palabra reservada de DynamoDB.
    """
    sets = {"status": new_status, **(extra_sets or {})}
    if gsi:
        sets["GSI1PK"] = f"RDM#{new_status}"
    names = {"#st": "status"}
    values = {":from": serializer.serialize(current)}
    parts = []
    for i, (attr, value) in enumerate(sets.items()):
        names[f"#s{i}"] = attr
        values[f":s{i}"] = serializer.serialize(value)
        parts.append(f"#s{i} = :s{i}")
    return {"Update": {
        "TableName": TABLE_NAME,
        "Key": {k: serializer.serialize(v) for k, v in key.items()},
        "UpdateExpression": "SET " + ", ".join(parts),
        "ConditionExpression": "#st = :from",
        "ExpressionAttributeNames": names,
        "ExpressionAttributeValues": values,
    }}


@app.patch("/admin/redemptions/<redemption_id>", middlewares=[require_group(ADMIN_GROUP)])
def update_redemption(redemption_id: str):
    """Cambia el estado de un canje.

    Body: {email, status}
        status: ENTREGADO (AWS) | APLICADO (institucional) | REEMBOLSADO (ambos)
    """
    redemption_id = redemption_id.lower()
    if not UUID_RE.match(redemption_id):
        raise BadRequestError("Identificador de canje no válido.")
    data = body()
    email = str(data.get("email", "")).strip().lower()
    new_status = str(data.get("status", "")).upper()
    if not STUDENT_EMAIL.match(email):
        raise BadRequestError("El correo del estudiante no es válido.")

    rdm = find_redemption(email, redemption_id)
    if not rdm:
        raise NotFoundError("El canje no existe.")
    current = rdm["status"]
    if new_status not in TRANSITIONS.get(current, set()):
        raise ConflictError(f"Un canje en estado {current} no puede pasar a {new_status or 'ese estado'}.")

    admin = app.context["email"]
    now = now_iso()
    rdm_key = {"PK": rdm["PK"], "SK": rdm["SK"]}
    coupon_key = {"PK": f"COUPON#{redemption_id}", "SK": "META"}
    institutional = rdm["type"] == "INSTITUCIONAL"

    if new_status == "REEMBOLSADO":
        cost = int(rdm["points"])
        items = [
            # 0. El canje pasa a reembolsado (solo si nadie lo cambió mientras tanto)
            set_status_item(rdm_key, current, new_status,
                            {"refundedAt": now, "refundedBy": admin, "updatedAt": now, "updatedBy": admin}, gsi=True),
            # 1. Devolver los puntos
            {"Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"USER#{email}"}, "SK": {"S": "PROFILE"}},
                "UpdateExpression": "SET points = if_not_exists(points, :zero) + :cost",
                "ExpressionAttributeValues": {":zero": {"N": "0"}, ":cost": {"N": str(cost)}},
            }},
            # 2. Movimiento positivo en el historial
            put_new({
                "PK": f"USER#{email}",
                "SK": f"ACT#{now}#refund-{redemption_id}",
                "date": now,
                "title": f"Reembolso: {rdm['rewardName']}",
                "points": cost,
                "kind": "refund",  # no cuenta como actividad realizada en los vales
            }),
        ]
        # 3. Regresar la unidad al stock (si la recompensa sigue en la tabla)
        reward_key = {"PK": f"REWARD#{rdm['rewardId']}", "SK": "META"}
        if table.get_item(Key=reward_key).get("Item"):
            items.append({"Update": {
                "TableName": TABLE_NAME,
                "Key": {k: serializer.serialize(v) for k, v in reward_key.items()},
                "UpdateExpression": "SET stock = stock + :one",
                "ConditionExpression": "attribute_exists(PK)",
                "ExpressionAttributeValues": {":one": {"N": "1"}},
            }})
        # 4. El vale deja de ser válido
        if institutional:
            items.append(set_status_item(coupon_key, current, new_status, {"refundedAt": now}))
    else:
        items = [set_status_item(rdm_key, current, new_status, {"updatedAt": now, "updatedBy": admin}, gsi=True)]
        if institutional:  # APLICADO también se refleja en la verificación del vale
            items.append(set_status_item(coupon_key, current, new_status, {"updatedAt": now}))

    try:
        ddb.transact_write_items(TransactItems=items)
    except ClientError as e:
        if e.response["Error"]["Code"] != "TransactionCanceledException":
            raise
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        if reasons and reasons[0] == "ConditionalCheckFailed":
            raise ConflictError("El estado del canje cambió. Recarga la lista.")
        if "TransactionConflict" in reasons:
            raise ConflictError("El canje se está actualizando. Intenta de nuevo.")
        logger.error("Cambio de estado cancelado", extra={"razones": reasons})
        raise ConflictError("No se pudo actualizar el canje.")

    logger.info("Canje actualizado", extra={"canje": redemption_id, "de": current, "a": new_status})
    updated = table.get_item(Key=rdm_key, ConsistentRead=True)["Item"]
    return clean(pick(updated, ADMIN_FIELDS))


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
