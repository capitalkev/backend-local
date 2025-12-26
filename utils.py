import base64
import email
from typing import Dict, List
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders

from config import USER_ID


def obtener_mensaje_parseado(servicio, mensaje_id: str):
    """
    Obtiene un mensaje de Gmail y lo parsea a formato email.Message

    Args:
        servicio: Servicio de Gmail API
        mensaje_id: ID del mensaje a obtener

    Returns:
        email.Message: Mensaje parseado
    """
    mensaje_raw = (
        servicio.users()
        .messages()
        .get(userId=USER_ID, id=mensaje_id, format="raw")
        .execute()
    )

    raw_message = mensaje_raw["raw"]
    mensaje_bytes = base64.urlsafe_b64decode(raw_message)
    mensaje_str = mensaje_bytes.decode("utf-8", errors="ignore")

    return email.message_from_string(mensaje_str)


def extraer_header(headers: List[Dict], nombre: str, default="") -> str:
    """
    Extrae un header ignorando mayúsculas/minúsculas (Case Insensitive)
    """
    nombre_buscado = nombre.lower()
    return next(
        (h["value"] for h in headers if h["name"].lower() == nombre_buscado), default
    )


def copiar_contenido_mensaje(mensaje_parseado, nuevo_mensaje: MIMEMultipart):
    """
    Copia el contenido completo (texto, HTML y adjuntos) de un mensaje parseado a un nuevo mensaje

    Args:
        mensaje_parseado: Mensaje original parseado
        nuevo_mensaje: Nuevo mensaje donde copiar el contenido
    """
    if mensaje_parseado.is_multipart():
        for parte in mensaje_parseado.walk():
            content_type = parte.get_content_type()
            content_disposition = str(parte.get("Content-Disposition", ""))

            if content_type == "text/plain" and "attachment" not in content_disposition:
                cuerpo = parte.get_payload(decode=True).decode(errors="ignore")
                nuevo_mensaje.attach(MIMEText(cuerpo, "plain"))
            elif (
                content_type == "text/html" and "attachment" not in content_disposition
            ):
                cuerpo = parte.get_payload(decode=True).decode(errors="ignore")
                nuevo_mensaje.attach(MIMEText(cuerpo, "html"))
            elif "attachment" in content_disposition or parte.get_filename():
                maintype, subtype = content_type.split("/", 1)
                adjunto = MIMEBase(maintype, subtype)
                adjunto.set_payload(parte.get_payload(decode=True))
                encoders.encode_base64(adjunto)

                filename = parte.get_filename()
                if filename:
                    adjunto.add_header(
                        "Content-Disposition", "attachment", filename=filename
                    )
                nuevo_mensaje.attach(adjunto)
    else:
        cuerpo = mensaje_parseado.get_payload(decode=True).decode(errors="ignore")
        nuevo_mensaje.attach(MIMEText(cuerpo, "plain"))


def enviar_mensaje_gmail(service, message, thread_id=None):
    try:
        encoded_message = base64.urlsafe_b64encode(message.as_bytes()).decode()

        body = {"raw": encoded_message}

        if thread_id:
            body["threadId"] = thread_id

        message_sent = service.users().messages().send(userId="me", body=body).execute()

        return message_sent
    except Exception as e:
        print(f"Error: {e}")
        return None
