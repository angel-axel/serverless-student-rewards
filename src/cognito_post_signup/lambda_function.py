"""
Registro - Student Builder Group rewards platform

Trigger "Posterior a la confirmación" (Post confirmation) del User Pool de Cognito.

Cuando un estudiante confirma su cuenta con el código del correo:
    1. Lo agrega al grupo "miembro" (necesario para canjear y suscribirse).
    2. Crea su perfil con 0 puntos.
    3. Si OFFICIAL_PRESENTATION=true, le da un bono de bienvenida de 100 puntos
       (presentación oficial del grupo). El bono queda en su historial
       y se da una sola vez por perfil.

Solo actúa en PostConfirmation_ConfirmSignUp; al recuperar contraseña
(PostConfirmation_ConfirmForgotPassword) no hace nada.

Cognito puede reintentar el trigger, así que cada paso es idempotente: el perfil
se crea con if_not_exists y el bono se protege con welcomeBonusAt.

Si algo falla no se bloquea la confirmación (la cuenta ya quedó confirmada en
Cognito): se deja el error en CloudWatch para corregirlo a mano.

Variables de entorno (todas obligatorias):
    TABLE_NAME              tabla de DynamoDB
    OFFICIAL_PRESENTATION   "true" para dar el bono de bienvenida, "false" para no darlo
"""

import logging
import os
import re
import time
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]

_presentation = os.environ["OFFICIAL_PRESENTATION"].strip().lower()
if _presentation not in ("true", "false"):
    raise ValueError('OFFICIAL_PRESENTATION debe ser "true" o "false".')
OFFICIAL_PRESENTATION = _presentation == "true"

MEMBER_GROUP = "miembro"

WELCOME_BONUS_POINTS = 100
WELCOME_BONUS_TITLE = "Bono de bienvenida: presentación oficial del AWS Student Builder Group"

# Misma regla que el trigger previo al registro, como segunda barrera
INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()
STUDENT_EMAIL = re.compile(r"^(?=[^@]*\d)[a-z0-9._-]+@" + re.escape(INSTITUTIONAL_DOMAIN) + "$")

# Clientes fuera del handler para reutilizarlos entre invocaciones
cognito = boto3.client("cognito-idp")
table = boto3.resource("dynamodb").Table(TABLE_NAME)
ddb = boto3.client("dynamodb")  # transact_write_items solo existe en el client


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #

def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def mask(email):
    """Oculta el correo en los logs: l00000000@... -> l0***@..."""
    local, _, domain = (email or "").partition("@")
    return f"{local[:2]}***@{domain}" if domain else "desconocido"


# --------------------------------------------------------------------------- #
# Pasos del registro
# --------------------------------------------------------------------------- #

def add_to_group(user_pool_id, username, attempts=3):
    """Agrega al grupo de miembros. Es idempotente: si ya pertenece no pasa nada."""
    for attempt in range(1, attempts + 1):
        try:
            cognito.admin_add_user_to_group(UserPoolId=user_pool_id, Username=username, GroupName=MEMBER_GROUP)
            return True
        except ClientError as e:
            code = e.response["Error"]["Code"]
            # Errores de configuración: reintentar no sirve
            if code in ("ResourceNotFoundException", "AccessDeniedException", "UserNotFoundException"):
                logger.error("No se pudo agregar al grupo %s: %s", MEMBER_GROUP, code)
                return False
            logger.warning("Intento %s fallido (%s), reintentando", attempt, code)
            time.sleep(0.3 * attempt)
    return False


def create_profile(email):
    """Crea el perfil con 0 puntos. if_not_exists evita reiniciarlo si el trigger se repite."""
    now = now_iso()
    table.update_item(
        Key={"PK": f"USER#{email}", "SK": "PROFILE"},
        UpdateExpression=(
            "SET points = if_not_exists(points, :zero), "
            "subscribed = if_not_exists(subscribed, :false), "
            "createdAt = if_not_exists(createdAt, :now), "
            "email = :email"
        ),
        ExpressionAttributeValues={":zero": 0, ":false": False, ":now": now, ":email": email},
    )


def grant_welcome_bonus(email):
    """Suma el bono al perfil y lo registra en el historial, en una sola transacción.

    welcomeBonusAt en el perfil evita darlo dos veces (por ejemplo, si Cognito
    reintenta el trigger). Regresa False si el alumno ya lo había recibido.
    """
    now = now_iso()
    try:
        ddb.transact_write_items(TransactItems=[
            # Perfil: crea o suma, solo si no ha recibido el bono
            {"Update": {
                "TableName": TABLE_NAME,
                "Key": {"PK": {"S": f"USER#{email}"}, "SK": {"S": "PROFILE"}},
                "UpdateExpression": (
                    "SET points = if_not_exists(points, :zero) + :bonus, "
                    "subscribed = if_not_exists(subscribed, :false), "
                    "createdAt = if_not_exists(createdAt, :now), "
                    "email = :email, welcomeBonusAt = :now"
                ),
                "ConditionExpression": "attribute_not_exists(welcomeBonusAt)",
                "ExpressionAttributeValues": {
                    ":zero": {"N": "0"},
                    ":bonus": {"N": str(WELCOME_BONUS_POINTS)},
                    ":false": {"BOOL": False},
                    ":now": {"S": now},
                    ":email": {"S": email},
                },
            }},
            # Movimiento en el historial del alumno
            {"Put": {
                "TableName": TABLE_NAME,
                "Item": {
                    "PK": {"S": f"USER#{email}"},
                    "SK": {"S": f"ACT#{now}#welcome-bonus"},
                    "date": {"S": now},
                    "title": {"S": WELCOME_BONUS_TITLE},
                    "points": {"N": str(WELCOME_BONUS_POINTS)},
                },
                "ConditionExpression": "attribute_not_exists(PK)",
            }},
        ])
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "TransactionCanceledException":
            reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
            if reasons and reasons[0] == "ConditionalCheckFailed":
                return False  # ya lo había recibido
        raise


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def lambda_handler(event, context):
    if event.get("triggerSource") != "PostConfirmation_ConfirmSignUp":
        return event

    email = (event.get("request", {}).get("userAttributes", {}).get("email") or "").strip().lower()
    if not STUDENT_EMAIL.match(email):
        # No debería pasar (lo bloquea el trigger previo al registro); no se le da grupo ni puntos
        logger.warning("Confirmación de un correo que no es de estudiante: %s", mask(email))
        return event

    if add_to_group(event["userPoolId"], event["userName"]):
        logger.info("Estudiante %s agregado al grupo %s", mask(email), MEMBER_GROUP)
    else:
        logger.error("ACCION MANUAL: agregar %s al grupo %s", mask(email), MEMBER_GROUP)

    try:
        if OFFICIAL_PRESENTATION:
            # La transacción también crea el perfil si no existe
            if grant_welcome_bonus(email):
                logger.info("Bono de bienvenida (+%s) para %s", WELCOME_BONUS_POINTS, mask(email))
            else:
                logger.info("%s ya había recibido el bono de bienvenida", mask(email))
        else:
            create_profile(email)
    except ClientError:
        logger.exception("No se pudo crear el perfil o dar el bono de %s", mask(email))

    return event
