"""
Avisos de eventos - Student Builder Group rewards platform

Envía correos (Amazon SES) a los estudiantes suscritos a los avisos de eventos.

Se invoca de dos formas:

1. DynamoDB Streams (solo items EVENT#..., filtrado en el event source mapping)
    INSERT  evento nuevo y futuro  -> aviso "Nuevo evento" + programa el recordatorio
    MODIFY  cambió nombre, descripción, lugar, fecha o puntos (evento futuro)
                                   -> aviso "Evento actualizado" con lo que cambió
            cambió la fecha        -> además reprograma el recordatorio
            se eliminó             -> borra el recordatorio
            se restauró            -> vuelve a programar el recordatorio
    REMOVE                         -> borra el recordatorio

2. EventBridge Scheduler (un horario de una sola vez por evento)
    El día del evento a las 8:00 (hora del centro de México) envía el recordatorio.
    Si el evento se crea ese mismo día después de las 8:00, el recordatorio sale en
    unos minutos. El horario se borra solo al ejecutarse.

Destinatarios: perfiles con GSI1PK = SUBSCRIBERS. Se envía un correo individual a
cada suscriptor (su dirección en el To), así nadie ve correos ajenos y no se envían
copias a direcciones que no existen (que contarían como rebotes en SES).
Cada correo lleva el encabezado List-Unsubscribe apuntando a la SPA.

Para no enviar dos veces (Streams y Scheduler pueden reintentar), antes de enviar
se marca el evento con notifiedAt / updateNotifiedFor / reminderSentFor usando
escrituras condicionales.
Si el envío falla, la marca se quita para que el reintento sí lo envíe.
Esas marcas generan otro MODIFY en el Stream, que se ignora porque no cambia ningún
dato del evento que se avise.

Variables de entorno (todas obligatorias):
    TABLE_NAME           tabla de DynamoDB
    SENDER_EMAIL         remitente verificado en SES. Ej.: Mi Grupo <no-reply@example.com>
    SITE_URL             URL de la SPA. Ej.: https://example.com/
    SCHEDULE_GROUP       grupo de EventBridge Scheduler. Ej.: event-reminders
    SCHEDULER_ROLE_ARN   rol que usa Scheduler para invocar esta Lambda
    GROUP_NAME           nombre del grupo que aparece en los correos
"""

import hashlib
import html
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key
from boto3.dynamodb.types import TypeDeserializer
from botocore.config import Config
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --------------------------------------------------------------------------- #
# Configuración
# --------------------------------------------------------------------------- #

TABLE_NAME = os.environ["TABLE_NAME"]
SENDER_EMAIL = os.environ["SENDER_EMAIL"]
SITE_URL = os.environ["SITE_URL"].rstrip("/")
SCHEDULE_GROUP = os.environ["SCHEDULE_GROUP"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
GROUP_NAME = os.environ["GROUP_NAME"]

# Centro de México: UTC-6 todo el año (sin horario de verano desde 2022)
MX_TZ = timezone(timedelta(hours=-6))
MX_TZ_NAME = "America/Mexico_City"
REMINDER_HOUR = 8  # hora local del recordatorio el día del evento

# Datos del evento cuyos cambios se avisan a los suscritos
TRACKED_FIELDS = {
    "title": "Nombre",
    "date": "Fecha",
    "location": "Lugar",
    "points": "Puntos",
    "description": "Descripción",
}

# Errores de SES que se deben a una dirección concreta: se omite ese destinatario
# y se sigue con los demás en lugar de reintentar todo el aviso
SKIP_RECIPIENT_ERRORS = {"MessageRejected", "BadRequestException", "NotFoundException"}

ISO_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

WEEKDAYS = ["lunes", "martes", "miércoles", "jueves", "viernes", "sábado", "domingo"]
MONTHS = ["enero", "febrero", "marzo", "abril", "mayo", "junio", "julio",
          "agosto", "septiembre", "octubre", "noviembre", "diciembre"]

# Reintentos con espera exponencial ante límites de envío de SES
retry_config = Config(retries={"mode": "standard", "max_attempts": 6})

table = boto3.resource("dynamodb").Table(TABLE_NAME)
ses = boto3.client("sesv2", config=retry_config)
scheduler = boto3.client("scheduler")
deserializer = TypeDeserializer()


# --------------------------------------------------------------------------- #
# Utilidades
# --------------------------------------------------------------------------- #

def now_utc():
    return datetime.now(timezone.utc)


def parse_utc(value):
    return datetime.strptime(value, ISO_FORMAT).replace(tzinfo=timezone.utc)


def human_date(iso):
    """'2026-10-21T22:00:00Z' -> 'miércoles 21 de octubre, 4:00 p. m.'"""
    d = parse_utc(iso).astimezone(MX_TZ)
    hour = d.hour % 12 or 12
    suffix = "a. m." if d.hour < 12 else "p. m."
    return f"{WEEKDAYS[d.weekday()]} {d.day} de {MONTHS[d.month - 1]}, {hour}:{d.minute:02d} {suffix}"


def from_image(image):
    """Imagen del Stream (formato DynamoDB) -> dict de Python."""
    return {k: deserializer.deserialize(v) for k, v in (image or {}).items()}


def schedule_name(event_id):
    """Los nombres de Scheduler tienen máximo 64 caracteres; el id del evento puede ser más largo."""
    return "event-reminder-" + hashlib.sha1(event_id.encode()).hexdigest()[:40]


# --------------------------------------------------------------------------- #
# Destinatarios y envío
# --------------------------------------------------------------------------- #

def subscribers():
    """Correos de los perfiles suscritos (índice disperso SUBSCRIBERS)."""
    emails, kwargs = [], {"IndexName": "GSI1", "KeyConditionExpression": Key("GSI1PK").eq("SUBSCRIBERS")}
    while True:
        page = table.query(**kwargs)
        emails += [i["email"] for i in page["Items"] if i.get("subscribed") is True and i.get("email")]
        if "LastEvaluatedKey" not in page:
            return sorted(set(emails))
        kwargs["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def shown(field, value):
    """Valor de un campo tal como se muestra en el correo."""
    if field == "date":
        return human_date(value) if value else "(sin fecha)"
    if field == "points":
        return f"+{int(value or 0)}"
    if field == "description":
        return value or "(sin descripción)"
    return value or ""


def describe_changes(old, new):
    """[(campo, antes, ahora)] de los datos que cambiaron, listos para mostrarse."""
    return [
        (label, shown(field, old.get(field)), shown(field, new.get(field)))
        for field, label in TRACKED_FIELDS.items()
        if shown(field, old.get(field)) != shown(field, new.get(field))
    ]


def build_email(event, kind, changes=None):
    """Asunto, texto y HTML del aviso. kind: 'new', 'update' o 'reminder'."""
    title = event["title"]
    when = human_date(event["date"])
    location = event.get("location", "")
    points = int(event.get("points", 0))
    description = event.get("description", "")

    if kind == "new":
        subject = f"Nuevo evento: {title}"
        intro = f"Se publicó un nuevo evento en {GROUP_NAME}."
    elif kind == "update":
        subject = f"Evento actualizado: {title}"
        intro = "Se actualizó la información de un evento. Estos son los cambios:"
    else:
        subject = f"Hoy: {title}"
        intro = "Te recordamos que este evento es hoy. ¡Te esperamos!"

    footer = f"Recibes este correo porque activaste los avisos por correo. Puedes desactivarlos en {SITE_URL}"

    changes = changes or []
    lines = [intro, ""]
    for label, before, after in changes:
        lines += [f"{label}: {after}", f"  (antes: {before})"]
    if changes:
        lines += ["", "Así queda el evento:", ""]
    lines += [title, f"Cuándo: {when}", f"Dónde: {location}", f"Puntos: +{points}"]
    if description:
        lines += ["", description]
    lines += ["", f"Más información: {SITE_URL}", "", footer]
    text = "\n".join(lines)

    e = html.escape
    changes_html = ""
    if changes:
        rows = "".join(
            # Gmail y Outlook ignoran el padding de la tabla: el espacio va en cada celda
            f'<tr><td style="padding:12px 16px;color:#4B5563;vertical-align:top;white-space:nowrap"><b>{e(label)}</b></td>'
            f'<td style="padding:12px 16px 12px 0;line-height:1.5">'
            f'<span style="color:#16213A">{e(after)}</span><br>'
            f'<span style="color:#9CA3AF;text-decoration:line-through">{e(before)}</span></td></tr>'
            for label, before, after in changes
        )
        changes_html = (
            '<table role="presentation" style="width:100%;border-collapse:collapse;margin:0 0 28px;'
            f'background:#FFF7EC;border-left:4px solid #F29111">{rows}</table>'
            '<p style="margin:0 0 12px;color:#4B5563">Así queda el evento:</p>'
        )
    body = f"""<!doctype html>
<html lang="es-MX"><body style="margin:0;padding:24px;background:#F4F6F9;font-family:Arial,sans-serif;color:#16213A">
  <div style="max-width:520px;margin:0 auto;background:#fff;border-radius:12px;overflow:hidden;border:1px solid #DCE1EA">
    <div style="background:#16213A;color:#fff;padding:18px 24px;font-weight:bold">{e(GROUP_NAME)}</div>
    <div style="padding:24px">
      <p style="margin:0 0 {20 if changes else 16}px;color:#4B5563">{e(intro)}</p>
      {changes_html}
      <h1 style="margin:0 0 12px;font-size:22px">{e(title)}</h1>
      <p style="margin:4px 0"><b>Cuándo:</b> {e(when)}</p>
      <p style="margin:4px 0"><b>Dónde:</b> {e(location)}</p>
      <p style="margin:4px 0"><b>Puntos:</b> +{points}</p>
      {f'<p style="margin:16px 0 0;color:#374151">{e(description)}</p>' if description else ''}
      <p style="margin:24px 0 0"><a href="{e(SITE_URL)}" style="display:inline-block;background:#F29111;color:#16213A;padding:10px 18px;border-radius:8px;font-weight:bold;text-decoration:none">Ver eventos</a></p>
    </div>
    <div style="padding:16px 24px;font-size:12px;color:#6B7280;border-top:1px solid #DCE1EA">{e(footer)}</div>
  </div>
</body></html>"""
    return subject, text, body


def send_to_subscribers(event, kind, changes=None):
    """Envía el aviso a cada suscriptor por separado. Regresa cuántos se enviaron.

    Los límites de envío de SES (throttling) los reintenta botocore con espera
    exponencial. Si un destinatario concreto falla (dirección rechazada, etc.) se
    registra y se sigue con los demás. Cualquier otro error se propaga para que
    notify_once quite la marca y el origen (Stream o Scheduler) reintente.
    """
    recipients = subscribers()
    if not recipients:
        logger.info("Sin suscriptores para el aviso %s de %s", kind, event["id"])
        return 0

    subject, text, body = build_email(event, kind, changes)
    content = {"Simple": {
        "Subject": {"Data": subject, "Charset": "UTF-8"},
        "Body": {"Text": {"Data": text, "Charset": "UTF-8"},
                 "Html": {"Data": body, "Charset": "UTF-8"}},
        # Gmail y Outlook muestran un enlace "Cancelar suscripción" junto al remitente
        "Headers": [{"Name": "List-Unsubscribe", "Value": f"<{SITE_URL}>"}],
    }}

    sent, skipped = 0, 0
    for email in recipients:
        try:
            ses.send_email(
                FromEmailAddress=SENDER_EMAIL,
                Destination={"ToAddresses": [email]},
                Content=content,
            )
            sent += 1
        except ClientError as e:
            if e.response["Error"]["Code"] not in SKIP_RECIPIENT_ERRORS:
                raise
            skipped += 1
            logger.warning("Aviso %s de %s no enviado a un destinatario: %s",
                           kind, event["id"], e.response["Error"]["Message"])

    if sent == 0 and skipped:
        raise RuntimeError(f"SES rechazó todos los envíos del aviso {kind} de {event['id']}")

    logger.info("Aviso %s de %s: %s enviados, %s omitidos", kind, event["id"], sent, skipped)
    return sent


# --------------------------------------------------------------------------- #
# Marcas para no enviar dos veces
# --------------------------------------------------------------------------- #

def claim(event_id, attribute, value=None):
    """Marca el evento antes de enviar. Regresa False si otro intento ya lo marcó.

    notifiedAt:       se marca una sola vez por evento
    reminderSentFor:  guarda la fecha del evento; si se reprograma se puede volver a enviar
    """
    marker = value or now_utc().strftime(ISO_FORMAT)
    condition = "attribute_exists(PK) AND (attribute_not_exists(#a)"
    condition += " OR #a <> :v)" if value else ")"
    try:
        table.update_item(
            Key={"PK": f"EVENT#{event_id}", "SK": "META"},
            UpdateExpression="SET #a = :v",
            ConditionExpression=condition,
            ExpressionAttributeNames={"#a": attribute},
            ExpressionAttributeValues={":v": marker},
        )
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            return False
        raise


def release(event_id, attribute):
    """Quita la marca si el envío falló, para que el reintento lo vuelva a intentar."""
    try:
        table.update_item(
            Key={"PK": f"EVENT#{event_id}", "SK": "META"},
            UpdateExpression="REMOVE #a",
            ExpressionAttributeNames={"#a": attribute},
        )
    except ClientError:
        logger.exception("No se pudo quitar la marca %s de %s", attribute, event_id)


def notify_once(event, attribute, kind, value=None, changes=None):
    """Marca, envía y, si el envío falla, quita la marca y propaga el error."""
    if not claim(event["id"], attribute, value):
        logger.info("Aviso %s de %s ya enviado", kind, event["id"])
        return
    try:
        send_to_subscribers(event, kind, changes)
    except Exception:
        release(event["id"], attribute)
        raise


# --------------------------------------------------------------------------- #
# Recordatorios (EventBridge Scheduler)
# --------------------------------------------------------------------------- #

def reminder_time(event_date):
    """Hora local (MX) del recordatorio, o None si ya no tiene sentido enviarlo."""
    start = parse_utc(event_date)
    now = now_utc()
    if start <= now:
        return None
    local_start = start.astimezone(MX_TZ)
    at = local_start.replace(hour=REMINDER_HOUR, minute=0, second=0)
    if at >= local_start:  # evento antes de las 8:00: se avisa 2 horas antes
        at = local_start - timedelta(hours=2)
    if at <= now.astimezone(MX_TZ):  # creado tarde: se avisa en unos minutos
        at = now.astimezone(MX_TZ) + timedelta(minutes=2)
    return at if at < local_start else None


def upsert_reminder(event, function_arn):
    at = reminder_time(event["date"])
    if not at:
        delete_reminder(event["id"])
        return
    params = {
        "Name": schedule_name(event["id"]),
        "GroupName": SCHEDULE_GROUP,
        "ScheduleExpression": f"at({at.strftime('%Y-%m-%dT%H:%M:%S')})",
        "ScheduleExpressionTimezone": MX_TZ_NAME,
        "FlexibleTimeWindow": {"Mode": "OFF"},
        "ActionAfterCompletion": "DELETE",
        "Target": {
            "Arn": function_arn,
            "RoleArn": SCHEDULER_ROLE_ARN,
            "Input": json.dumps({"source": "event-reminder", "eventId": event["id"], "date": event["date"]}),
        },
    }
    try:
        scheduler.create_schedule(**params)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConflictException":
            raise
        scheduler.update_schedule(**params)  # ya existía: se reprograma
    logger.info("Recordatorio de %s programado para %s (hora de México)", event["id"], at.strftime("%Y-%m-%d %H:%M"))


def delete_reminder(event_id):
    try:
        scheduler.delete_schedule(Name=schedule_name(event_id), GroupName=SCHEDULE_GROUP)
        logger.info("Recordatorio de %s eliminado", event_id)
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise


# --------------------------------------------------------------------------- #
# Manejo de cada origen
# --------------------------------------------------------------------------- #

def is_event(item):
    return item.get("PK", "").startswith("EVENT#") and item.get("SK") == "META"


def handle_stream_record(record, function_arn):
    name = record["eventName"]
    new = from_image(record["dynamodb"].get("NewImage"))
    old = from_image(record["dynamodb"].get("OldImage"))
    item = new or old
    if not is_event(item):
        return

    if name == "REMOVE":
        delete_reminder(old["id"])
        return

    if new.get("deleted") is True:
        if name == "INSERT" or old.get("deleted") is not True:
            delete_reminder(new["id"])
        return

    is_future = parse_utc(new["date"]) > now_utc()

    if name == "INSERT":
        if not is_future:
            return  # evento pasado (registro de asistencia): no se avisa
        notify_once(new, "notifiedAt", "new")
        upsert_reminder(new, function_arn)
        return

    # MODIFY de un evento activo
    if old.get("deleted") is True:
        upsert_reminder(new, function_arn)  # restaurado: solo vuelve el recordatorio
        return

    changes = describe_changes(old, new)
    if not changes:
        return  # marcas internas (notifiedAt, updatedAt…) u otros datos que no se avisan

    if is_future:
        # updatedAt identifica esta edición; si un reintento trae la misma, no se reenvía
        edition = new.get("updatedAt") or hashlib.sha1(
            json.dumps([new.get(f) for f in TRACKED_FIELDS], default=str).encode()
        ).hexdigest()
        notify_once(new, "updateNotifiedFor", "update", edition, changes)

    if new.get("date") != old.get("date"):
        upsert_reminder(new, function_arn)


def handle_reminder(payload):
    event_id = payload["eventId"]
    event = table.get_item(Key={"PK": f"EVENT#{event_id}", "SK": "META"}, ConsistentRead=True).get("Item")
    if not event or event.get("deleted") is True:
        logger.info("Recordatorio omitido: %s no existe o fue eliminado", event_id)
        return
    if event["date"] != payload.get("date"):
        logger.info("Recordatorio omitido: %s cambió de fecha", event_id)  # ya hay otro programado
        return
    notify_once(event, "reminderSentFor", "reminder", event["date"])


# --------------------------------------------------------------------------- #
# Handler
# --------------------------------------------------------------------------- #

def lambda_handler(event, context):
    # Invocación de EventBridge Scheduler
    if event.get("source") == "event-reminder":
        handle_reminder(event)
        return {"ok": True}

    # Lote de DynamoDB Streams: se reportan solo los registros que fallaron
    # (requiere ReportBatchItemFailures en el event source mapping)
    failures = []
    for record in event.get("Records", []):
        try:
            handle_stream_record(record, context.invoked_function_arn)
        except Exception:
            logger.exception("Error procesando %s", record.get("eventID"))
            failures.append({"itemIdentifier": record["dynamodb"]["SequenceNumber"]})
    return {"batchItemFailures": failures}
