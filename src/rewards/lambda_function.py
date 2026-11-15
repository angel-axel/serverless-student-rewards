"""
Recompensas - Student Builder Group rewards platform

Lambda detrás de API Gateway (HTTP API, payload 2.0) para el catálogo
y la administración de recompensas. Los canjes viven en la Lambda de canjes.

Rutas:
    GET   /rewards                     pública         catálogo visible
    POST  /admin/uploads               administrador   URL prefirmada para subir imagen
    POST  /admin/rewards               administrador   crear recompensa
    GET   /admin/rewards               administrador   catálogo paginado: ?status=active|inactive&limit=&cursor=
    PATCH /admin/rewards/<reward_id>   administrador   editar cualquier campo (incluida la imagen)

La autenticación la resuelve el autorizador JWT de la HTTP API (firma, expiración,
emisor y audiencia). Aquí solo se valida el grupo de Cognito de cada ruta.
CORS se configura en la HTTP API.

Índice GSI1 de recompensas (más recientes primero):
    visibles   GSI1PK = REWARDS            GSI1SK = <createdAt>
    ocultas    GSI1PK = REWARDS#INACTIVE   GSI1SK = <createdAt>
Al ocultar o mostrar una recompensa cambia de partición, así el catálogo
público solo lee las visibles y el panel pagina cada grupo por separado.

Requiere la capa AWSLambdaPowertoolsPythonV3-python313-<arquitectura>.

Variables de entorno (todas obligatorias):
    TABLE_NAME        tabla de DynamoDB
    BUCKET_NAME       bucket de las imágenes
    IMAGE_PREFIX      carpeta de las imágenes. Ej.: "media/rewards/"
                      (sin diagonal al inicio, con diagonal al final)
    MAX_IMAGE_BYTES   tamaño máximo por imagen en MB, mínimo 5
    CLOUDFRONT_DISTRIBUTION_ID
                      distribución que sirve las imágenes (para invalidar las que se quitan)
"""

import base64
import json
import os
import re
import secrets
import unicodedata
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
from botocore.config import Config
from botocore.exceptions import ClientError

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]
BUCKET_NAME = os.environ["BUCKET_NAME"]
IMAGE_PREFIX = os.environ["IMAGE_PREFIX"]
CLOUDFRONT_DISTRIBUTION_ID = os.environ["CLOUDFRONT_DISTRIBUTION_ID"]

# La variable se define en MB para que sea más fácil de leer en la consola (Ej.: 5)
MAX_IMAGE_MB = int(os.environ["MAX_IMAGE_BYTES"])
if MAX_IMAGE_MB < 5:
    raise ValueError("MAX_IMAGE_BYTES debe ser de al menos 5 (MB).")
MAX_IMAGE_BYTES = MAX_IMAGE_MB * 1024 * 1024

ADMIN_GROUP = "administrador"

ALLOWED_TYPES = {"image/png": "png", "image/jpeg": "jpg", "image/webp": "webp"}
REWARD_TYPES = {"AWS", "INSTITUCIONAL"}
REWARD_ID_RE = re.compile(r"^[a-z0-9-]{1,60}$")

# Campos de una recompensa que se exponen fuera de la Lambda
PUBLIC_FIELDS = ("id", "name", "description", "points", "type", "stock", "imageUrl")
ADMIN_FIELDS = (*PUBLIC_FIELDS, "active", "createdAt")

# Particiones del índice GSI1 según visibilidad
REWARDS_ACTIVE = "REWARDS"
REWARDS_INACTIVE = "REWARDS#INACTIVE"
STATUS_PARTITIONS = {"active": REWARDS_ACTIVE, "inactive": REWARDS_INACTIVE}

PAGE_SIZE_MAX = 50

logger = Logger(service="recompensas")

# strip_prefixes permite servir la API detrás de CloudFront en /api/*
app = APIGatewayHttpResolver(strip_prefixes=["/api"])

# Clientes fuera del handler para reutilizarlos entre invocaciones
table = boto3.resource("dynamodb").Table(TABLE_NAME)
s3 = boto3.client("s3", config=Config(signature_version="s3v4", s3={"addressing_style": "virtual"}))
cloudfront = boto3.client("cloudfront")


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


def slugify(text, max_len=40):
    """'Camisa de AWS (morada)' -> 'camisa-de-aws-morada'"""
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"[^a-zA-Z0-9]+", "-", text).strip("-").lower()
    return text[:max_len].strip("-") or "recompensa"


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
# Rutas públicas
# --------------------------------------------------------------------------- #

@app.get("/rewards")
def public_catalog():
    """Catálogo visible completo. Solo lee la partición de recompensas visibles."""
    items = query_all(
        IndexName="GSI1",
        KeyConditionExpression=Key("GSI1PK").eq(REWARDS_ACTIVE),
        FilterExpression=Attr("active").eq(True),  # compatibilidad con recompensas del esquema anterior
    )
    return clean([pick(i, PUBLIC_FIELDS) for i in items])


# --------------------------------------------------------------------------- #
# Administración
# --------------------------------------------------------------------------- #

@app.post("/admin/uploads", middlewares=[require_group(ADMIN_GROUP)])
def create_upload():
    """URL prefirmada (POST) para subir la imagen directo a S3.

    Body: {contentType, filename, size}
    Respuesta: {upload: {url, fields}, imageUrl}
    """
    data = body()

    content_type = data.get("contentType", "")
    if content_type not in ALLOWED_TYPES:
        raise BadRequestError("Solo se permiten imágenes PNG, JPG o WebP.")

    if data.get("size") is not None and as_int(data["size"], "size", 1, 10**9) > MAX_IMAGE_BYTES:
        raise BadRequestError(f"La imagen pesa más de {MAX_IMAGE_MB} MB.")

    # El nombre lo genera el servidor para no sobrescribir archivos ni salir de la carpeta
    key = f"{IMAGE_PREFIX}{uuid.uuid4()}.{ALLOWED_TYPES[content_type]}"

    # Las condiciones las aplica S3: tipo de archivo y tamaño máximo
    upload = s3.generate_presigned_post(
        Bucket=BUCKET_NAME,
        Key=key,
        Fields={"Content-Type": content_type},
        Conditions=[
            {"Content-Type": content_type},
            ["content-length-range", 1, MAX_IMAGE_BYTES],
        ],
        ExpiresIn=300,
    )
    return {"upload": upload, "imageUrl": f"/{key}"}


def validate_name(value):
    name = str(value or "").strip()
    if not 3 <= len(name) <= 120:
        raise BadRequestError("El nombre debe tener entre 3 y 120 caracteres.")
    return name


def validate_description(value):
    description = str(value or "").strip()
    if len(description) > 400:
        raise BadRequestError("La descripción no puede pasar de 400 caracteres.")
    return description


def validate_type(value):
    rtype = str(value or "").upper()
    if rtype not in REWARD_TYPES:
        raise BadRequestError("El tipo debe ser AWS o INSTITUCIONAL.")
    return rtype


def validate_image(value):
    """Solo acepta imágenes subidas con /admin/uploads que existan en el bucket."""
    key = str(value).lstrip("/")
    if not key.startswith(IMAGE_PREFIX) or ".." in key:
        raise BadRequestError("La imagen no es válida.")
    try:
        s3.head_object(Bucket=BUCKET_NAME, Key=key)
    except ClientError:
        raise BadRequestError("La imagen no se encontró. Vuelve a subirla.")
    return f"/{key}"


def delete_image(image_url):
    """Borra una imagen reemplazada o quitada y la invalida en CloudFront.

    Cada subida usa un nombre nuevo (UUID), así que la SPA nunca muestra una imagen vieja:
    la invalidación solo evita que la URL anterior siga respondiendo desde la caché.
    Si algo falla solo se registra, porque la recompensa ya se guardó.
    """
    if not image_url:
        return
    path = "/" + str(image_url).lstrip("/")
    try:
        s3.delete_object(Bucket=BUCKET_NAME, Key=path.lstrip("/"))
    except ClientError:
        logger.warning("No se pudo borrar la imagen anterior", extra={"imagen": path})
    try:
        cloudfront.create_invalidation(
            DistributionId=CLOUDFRONT_DISTRIBUTION_ID,
            InvalidationBatch={
                "Paths": {"Quantity": 1, "Items": [path]},  # solo esta imagen; el resto de la caché se conserva
                "CallerReference": str(uuid.uuid4()),
            },
        )
    except ClientError:
        logger.warning("No se pudo invalidar la imagen en CloudFront", extra={"imagen": path})


@app.post("/admin/rewards", middlewares=[require_group(ADMIN_GROUP)])
def create_reward():
    """Crea una recompensa visible.

    Body: {name, description?, points, stock, type, imageUrl?}
    """
    data = body()
    name = validate_name(data.get("name"))
    description = validate_description(data.get("description"))
    points = as_int(data.get("points"), "points", 1, 100_000)
    stock = as_int(data.get("stock"), "stock", 0, 10_000)
    rtype = validate_type(data.get("type"))
    image_url = validate_image(data["imageUrl"]) if data.get("imageUrl") else None

    reward_id = f"{slugify(name)}-{secrets.token_hex(3)}"
    created_at = now_iso()
    item = {
        "PK": f"REWARD#{reward_id}",
        "SK": "META",
        "GSI1PK": REWARDS_ACTIVE,
        "GSI1SK": created_at,  # más recientes primero en el panel
        "id": reward_id,
        "name": name,
        "points": points,
        "type": rtype,
        "stock": stock,
        "active": True,
        "createdAt": created_at,
        "createdBy": app.context["email"],
    }
    if description:
        item["description"] = description
    if image_url:
        item["imageUrl"] = image_url

    table.put_item(Item=item, ConditionExpression="attribute_not_exists(PK)")
    logger.info("Recompensa creada", extra={"recompensa": reward_id, "tipo": rtype, "puntos": points})

    return clean(pick(item, ADMIN_FIELDS)), 201


@app.get("/admin/rewards", middlewares=[require_group(ADMIN_GROUP)])
def list_rewards():
    """Catálogo paginado, de la más reciente a la más antigua.

    Query: status=active|inactive (obligatorio), limit=1..50 (20), cursor=<nextCursor anterior>
    Respuesta: {items, nextCursor}  (nextCursor = null cuando ya no hay más)
    """
    status = app.current_event.get_query_string_value("status", "")
    if status not in STATUS_PARTITIONS:
        raise BadRequestError("status debe ser active o inactive.")
    items, next_cursor = page_query(STATUS_PARTITIONS[status])
    return {"items": clean([pick(i, ADMIN_FIELDS) for i in items]), "nextCursor": next_cursor}


@app.patch("/admin/rewards/<reward_id>", middlewares=[require_group(ADMIN_GROUP)])
def update_reward(reward_id: str):
    """Edita cualquier campo de una recompensa. Solo se cambian los campos enviados.

    Body: {name?, description?, points?, stock?, type?, active?, imageUrl?}
        description: "" la quita
        imageUrl:    null la quita; una ruta nueva la reemplaza

    Los canjes y vales ya emitidos no cambian: guardan su propia copia de los datos.
    """
    if not REWARD_ID_RE.match(reward_id):
        raise BadRequestError("Identificador de recompensa no válido.")
    data = body()
    key = {"PK": f"REWARD#{reward_id}", "SK": "META"}

    current = table.get_item(Key=key, ConsistentRead=True).get("Item")
    if not current:
        raise NotFoundError("La recompensa no existe.")

    # Se usan alias (#x) porque "name" y "type" son palabras reservadas de DynamoDB
    sets, removes = {}, []

    if "name" in data:
        sets["name"] = validate_name(data["name"])
    if "description" in data:
        description = validate_description(data["description"])
        if description:
            sets["description"] = description
        else:
            removes.append("description")
    if "points" in data:
        sets["points"] = as_int(data["points"], "points", 1, 100_000)
    if "stock" in data:
        sets["stock"] = as_int(data["stock"], "stock", 0, 10_000)
    if "type" in data:
        sets["type"] = validate_type(data["type"])
    if "active" in data:
        if not isinstance(data["active"], bool):
            raise BadRequestError("active debe ser true o false.")
        sets["active"] = data["active"]

    old_image = current.get("imageUrl")
    image_changed = False
    if "imageUrl" in data:
        if data["imageUrl"]:
            new_image = validate_image(data["imageUrl"])
            if new_image != old_image:
                sets["imageUrl"] = new_image
                image_changed = True
        elif old_image:
            removes.append("imageUrl")
            image_changed = True

    if not sets and not removes:
        raise BadRequestError("No hay cambios que guardar.")

    # Visible u oculta decide la partición; el orden es por fecha de creación
    active = sets.get("active", current.get("active") is True)
    sets["GSI1PK"] = REWARDS_ACTIVE if active else REWARDS_INACTIVE
    sets["GSI1SK"] = current.get("createdAt") or now_iso()
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
            ConditionExpression="attribute_exists(PK)",  # evita crear la recompensa si se borró
            ReturnValues="ALL_NEW",
        )
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            raise NotFoundError("La recompensa no existe.")
        raise

    if image_changed:
        delete_image(old_image)

    logger.info("Recompensa actualizada", extra={"recompensa": reward_id,
                                                 "campos": sorted(k for k in data if k in ADMIN_FIELDS)})
    return clean(pick(res["Attributes"], ADMIN_FIELDS))


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
