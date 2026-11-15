"""
Perfil - Student Builder Group rewards platform

Lambda detrás de API Gateway (HTTP API, payload 2.0) con los datos del estudiante.

Rutas (todas para el grupo "miembro"):
    GET   /me              correo y saldo de puntos
    GET   /me/activities   historial de movimientos de puntos, paginado: ?limit=&cursor=

Los movimientos (USER#<correo> / ACT#<fecha>#<id>) los escriben otras partes del
sistema: el bono de bienvenida, el registro de asistencia y el canje de recompensas.
Aquí solo se leen.

La autenticación la resuelve el autorizador JWT de la HTTP API. Aquí solo se valida
el grupo de Cognito. CORS se configura en la HTTP API.

Requiere la capa AWSLambdaPowertoolsPythonV3-python313-<arquitectura>.

Variables de entorno (todas obligatorias):
    TABLE_NAME   tabla de DynamoDB
"""

import base64
import json
import os
import re
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

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]

MEMBER_GROUP = "miembro"

# Correo institucional con número de control antes de la @
INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()
STUDENT_EMAIL = re.compile(r"^(?=[^@]*\d)[a-z0-9._-]+@" + re.escape(INSTITUTIONAL_DOMAIN) + "$")

USER_PAGE_MAX = 50

logger = Logger(service="perfil")

# strip_prefixes permite servir la API detrás de CloudFront en /api/*
app = APIGatewayHttpResolver(strip_prefixes=["/api"])

table = boto3.resource("dynamodb").Table(TABLE_NAME)


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


def mask(email):
    """Oculta el correo en los logs: l00000000@... -> l0***@..."""
    local, _, domain = (email or "").partition("@")
    return f"{local[:2]}***@{domain}" if domain else "desconocido"


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


def student_email():
    """Correo del miembro autenticado; solo estudiantes tienen perfil."""
    email = app.context["email"]
    if not STUDENT_EMAIL.match(email):
        raise ForbiddenError("Solo los estudiantes tienen perfil en esta plataforma.")
    return email


# --------------------------------------------------------------------------- #
# Rutas
# --------------------------------------------------------------------------- #

@app.get("/me", middlewares=[require_group(MEMBER_GROUP)])
def get_profile():
    """Respuesta: {email, points}

    Si el perfil aún no existe (por ejemplo, si falló el trigger de confirmación)
    se responde con 0 puntos en lugar de un error.
    """
    email = student_email()
    profile = table.get_item(Key={"PK": f"USER#{email}", "SK": "PROFILE"}).get("Item") or {}
    return clean({"email": email, "points": profile.get("points", 0)})


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


@app.get("/me/activities", middlewares=[require_group(MEMBER_GROUP)])
def get_activities():
    """Historial de movimientos de puntos, del más reciente al más antiguo.

    Respuesta: {items: [{id, date, title, points}], nextCursor}  (points negativo en los canjes)
    """
    email = student_email()
    items, next_cursor = user_page(email, "ACT#")
    return {
        "items": clean([
            {
                "id": i["SK"].rsplit("#", 1)[-1],
                "date": i.get("date", ""),
                "title": i.get("title", ""),
                "points": i.get("points", 0),
            }
            for i in items
        ]),
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
