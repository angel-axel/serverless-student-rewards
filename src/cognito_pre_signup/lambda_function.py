"""
Registro - Student Builder Group rewards platform

Trigger "Previo al registro" (Pre sign-up) del User Pool de Cognito.

Solo permite registrarse a estudiantes: correo del dominio institucional
(variable de entorno INSTITUTIONAL_DOMAIN) con número de control antes de la @.
    Ej.: l00000000@example.edu

Para rechazar un registro basta con lanzar una excepción: Cognito cancela
el alta y muestra el mensaje al usuario (con el prefijo "PreSignUp failed with error").

Se ejecuta en todos los registros, incluidos los usuarios creados desde la consola.
"""

import logging
import os
import re

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

INSTITUTIONAL_DOMAIN = os.environ["INSTITUTIONAL_DOMAIN"].strip().lower()

MSG_NO_EMAIL = "Debes registrarte con tu correo institucional."
MSG_DOMAIN = f"Solo se permite el registro con tu correo institucional @{INSTITUTIONAL_DOMAIN}."
MSG_NOT_STUDENT = "Solo los estudiantes tienen permitido registrarse en esta plataforma."


# --------------------------------------------------------------------------- #
# Validación
# --------------------------------------------------------------------------- #

def mask(email):
    """Oculta el correo en los logs: l00000000@... -> l0***@..."""
    local, _, domain = email.partition("@")
    return f"{local[:2]}***@{domain}" if domain else "***"


def validate_email(email):
    """Lanza ValueError con el mensaje para el usuario si el correo no es de un estudiante."""
    if not email or email.count("@") != 1:
        raise ValueError(MSG_NO_EMAIL)

    local, domain = email.split("@")

    # Dominio exacto: rechaza subdominios y dominios parecidos
    # (alumno.example.edu, example.edu.com, etc.)
    if domain != INSTITUTIONAL_DOMAIN:
        raise ValueError(MSG_DOMAIN)

    # Los correos de estudiante llevan el número de control; los de docentes y personal no
    if not re.search(r"\d", local):
        raise ValueError(MSG_NOT_STUDENT)


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def lambda_handler(event, context):
    email = (
        event.get("request", {})
        .get("userAttributes", {})
        .get("email", "")
        .strip()
        .lower()
    )

    try:
        validate_email(email)
    except ValueError as err:
        logger.info("Registro rechazado: %s (%s) - %s", mask(email), event.get("triggerSource"), err)
        raise Exception(str(err))

    logger.info("Registro permitido: %s", mask(email))
    return event
