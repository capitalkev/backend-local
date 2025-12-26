import os.path
import mimetypes
import logging
from typing import Optional, Dict
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from fastapi import Depends
from sqlalchemy.orm import Session

from google.oauth2.credentials import Credentials
from googleapiclient.errors import HttpError
from googleapiclient.discovery import build

from database import get_db

from config import USER_ID

from utils import (
    obtener_mensaje_parseado,
    extraer_header,
    copiar_contenido_mensaje,
    enviar_mensaje_gmail,
)

from crud import (
    obtener_operacion_completa,
)

SCOPES = ['https://www.googleapis.com/auth/gmail.modify']

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def autenticar_gmail():
    """
    Autentica y crea el servicio de Gmail API

    Returns:
        Resource: Servicio de Gmail API autenticado
    """
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    return build('gmail', 'v1', credentials=creds)


def enviar_correo_multiples(servicio, destinatarios: str, op_id: str, db: Session = Depends(get_db), drive_url: str = None) -> Optional[Dict]:
    """
    Envía un correo electrónico a uno o múltiples destinatarios usando Gmail API

    Args:
        servicio: Servicio de Gmail API
        destinatarios: Emails separados por comas (ej: "email1@test.com,email2@test.com")

    Returns:
        Dict: {"mensaje_id": str, "destinatarios": list, "thread_id": str} o None si hay error
    """
    try:
        lista_destinatarios = [
            email.strip() for email in destinatarios.split(",") if email.strip()
        ]
        if not lista_destinatarios:
            logger.error("No se proporcionaron destinatarios válidos")
            return None

        mensaje = MIMEMultipart()
        mensaje["to"] = ", ".join(lista_destinatarios)
        mensaje["from"] = USER_ID
        mensaje["subject"] = "Prueba de Envío para Reenvío (ID Test)"

        datos_completos = obtener_operacion_completa(db, op_id)
        if not datos_completos:
            logger.error(f"No se encontró la operación con ID: {op_id}")
            return None

        cuerpo = cuerpo_template(datos_completos, destinatarios)

        mensaje.attach(cuerpo)

        archivo = "prueba.txt"
        ctype, encoding = mimetypes.guess_type(archivo)
        if ctype is None or encoding is not None:
            ctype = "application/octet-stream"
        maintype, subtype = ctype.split("/", 1)

        with open(archivo, "rb") as fp:
            msg_adjunto = MIMEBase(maintype, subtype)
            msg_adjunto.set_payload(fp.read())

        encoders.encode_base64(msg_adjunto)
        msg_adjunto.add_header("Content-Disposition", "attachment", filename=archivo)
        mensaje.attach(msg_adjunto)

        resultado = enviar_mensaje_gmail(servicio, mensaje)

        if resultado:
            mensaje_id = resultado["id"]
            thread_id = resultado.get("threadId")
            logger.info(f"Correo enviado a {len(lista_destinatarios)} destinatario(s). ID: {mensaje_id}")
            return {
                "mensaje_id": mensaje_id,
                "destinatarios": lista_destinatarios,
                "thread_id": thread_id,
            }

        logger.warning("No se pudo enviar el correo")
        return None

    except FileNotFoundError:
        logger.error(f"Archivo '{archivo}' no encontrado")
        return None
    except Exception as error:
        logger.error(f"Error al enviar correo: {error}")
        return None


def reenviar_a_multiples_destinatarios(
    servicio, mensaje_id: str, destinatarios: str
) -> Optional[Dict]:
    try:
        mensaje_completo = (
            servicio.users()
            .messages()
            .get(userId=USER_ID, id=mensaje_id, format="full")
            .execute()
        )

        thread_id = mensaje_completo.get("threadId")
        headers = mensaje_completo.get("payload", {}).get("headers", [])
        message_id_original = extraer_header(headers, "Message-ID")
        references_original = extraer_header(headers, "References")
        asunto_original = extraer_header(headers, "Subject") #

        if not asunto_original:
            logger.warning("ALERTA: No se encontró asunto original. El hilo podría romperse.")
            asunto_original = "Respuesta"

        lista_destinatarios = [
            email.strip() for email in destinatarios.split(",") if email.strip()
        ]
        if not lista_destinatarios:
            return None
        
        nuevo_mensaje = MIMEMultipart()
        nuevo_mensaje["to"] = ", ".join(lista_destinatarios)
        nuevo_mensaje["from"] = USER_ID
        nuevo_mensaje["subject"] = asunto_original

        if message_id_original:
            if not message_id_original.startswith("<"):
                message_id_original = f"<{message_id_original}>"
            
            nuevo_mensaje["In-Reply-To"] = message_id_original
            
            if references_original:
                nuevo_mensaje["References"] = f"{references_original} {message_id_original}"
            else:
                nuevo_mensaje["References"] = message_id_original

        mensaje_parseado = obtener_mensaje_parseado(servicio, mensaje_id)
        copiar_contenido_mensaje(mensaje_parseado, nuevo_mensaje)
        resultado = enviar_mensaje_gmail(servicio, nuevo_mensaje, thread_id)

        if resultado:
            logger.info(f"Respuesta anidada enviada al Thread: {thread_id}")
            return {
                "mensaje_id": resultado["id"],
                "destinatarios": lista_destinatarios,
                "thread_id": thread_id,
            }

        return None

    except HttpError as error:
        logger.error(f"Error Gmail API: {error}")
        return None
    except Exception as error:
        logger.error(f"Error inesperado: {error}")
        return None
 
 
def format_chilean_number(number, decimals=0):
    """
    Formatea un número con el formato chileno: punto para miles, coma para decimales

    Args:
        number: Número a formatear
        decimals: Cantidad de decimales (default 0)

    Returns:
        String formateado con formato chileno
    """
    try:
        num_float = float(number)

        # Formatear con separador de miles
        if decimals > 0:
            formatted = f"{num_float:,.{decimals}f}"
        else:
            formatted = f"{num_float:,.0f}"

        # Cambiar coma por punto (miles) y punto por coma (decimales)
        # Primero guardamos los decimales si existen
        if '.' in formatted:
            parts = formatted.split('.')
            integer_part = parts[0].replace(',', '.')
            decimal_part = parts[1]
            return f"{integer_part},{decimal_part}"
        else:
            return formatted.replace(',', '.')
    except (ValueError, TypeError):
        return str(number)


def format_date_short(date_str: str) -> str:
    """
    Convierte una fecha en formato YYYY-MM-DD a DD-MMM (ej: 02-oct)

    Args:
        date_str: Fecha en formato YYYY-MM-DD

    Returns:
        Fecha formateada como DD-MMM
    """
    if not date_str or date_str == 'N/A':
        return 'N/A'

    try:
        # Mapeo de meses en español (abreviado, minúsculas)
        meses = {
            1: 'ene', 2: 'feb', 3: 'mar', 4: 'abr',
            5: 'may', 6: 'jun', 7: 'jul', 8: 'ago',
            9: 'sep', 10: 'oct', 11: 'nov', 12: 'dic'
        }

        # Parsear la fecha en formato YYYY-MM-DD
        fecha = datetime.strptime(date_str, '%Y-%m-%d')

        # Formatear como DD-MMM
        dia = fecha.day
        mes = meses[fecha.month]

        return f"{dia:02d}-{mes}"
    except:
        return date_str

def cuerpo_template(datos_completos: dict, destinatarios: str):
    """
    Genera el cuerpo HTML del correo a partir de los datos completos de la operación.

    Args:
        datos_completos: Diccionario con 'operacion' y 'deudores'
        destinatarios: String con emails separados por comas

    Returns:
        MIMEText: Objeto MIME con el HTML del correo
    """
    operacion = datos_completos.get("operacion", {})
    deudores = datos_completos.get("deudores", [])

    emisor_razon_social = operacion.get("emisor_razon_social", "N/A")
    emisor_rut = operacion.get("emisor_rut", "N/A")
    operation_id = operacion.get("operation_id", "N/A")

    # Generar tabla HTML iterando sobre deudores y sus facturas
    tabla_html = """
    <table class="invoice_table">
        <thead>
            <tr>
                <th>RAZÓN SOCIAL PAGADOR</th>
                <th>FACTURA</th>
                <th>MONTO</th>
                <th>FECHA DE EMISION</th>
            </tr>
        </thead>
        <tbody>
    """

    # Iterar sobre cada deudor y sus facturas
    for deudor in deudores:
        nombre_deudor = deudor.get("nombre", "N/A")
        facturas = deudor.get("facturas", [])

        for factura in facturas:
            folio = factura.get("folio", "N/A")
            monto = factura.get("montoFactura", 0)
            monto_formatted = format_chilean_number(monto)
            fecha_emision = factura.get("fechaEmision", "N/A")
            fecha_formatted = format_date_short(fecha_emision)

            tabla_html += f"""
                <tr>
                    <td>{nombre_deudor}</td>
                    <td>{folio}</td>
                    <td>$ {monto_formatted}</td>
                    <td>{fecha_formatted}</td>
                </tr>
            """

    tabla_html += "</tbody></table>"

    # Generar lista de correos de contacto
    lista_destinatarios = [email.strip() for email in destinatarios.split(",") if email.strip()]
    correos_html = ""
    if lista_destinatarios and len(lista_destinatarios) > 0:
        correos_html = """
        <p><strong>Correos de contacto del receptor (para seguimiento):</strong></p>
        <ul style="margin-top: 5px; margin-bottom: 15px;">
        """
        for correo in lista_destinatarios:
            correos_html += f"<li>{correo}</li>\n"
        correos_html += "</ul>"

    # Generar el HTML completo del correo
    mensaje_html = f"""
    <!DOCTYPE html>
    <html lang="es">
    <head>
    <meta charset="UTF-8">
    <style>
        body {{ font-family: Arial, Helvetica, sans-serif; font-size: 13px; color: #000; }}
        .container {{ max-width: 800px; }}
        p, li {{ line-height: 1.5; }}
        ol {{ padding-left: 30px; }}
        table.invoice_table {{ width: 70%; border-collapse: collapse; margin-top: 15px; margin-bottom: 20px; }}
        table.invoice_table th, table.invoice_table td {{ border: 1px solid #777; padding: 6px; text-align: left; font-size: 12px; }}
        table.invoice_table th {{ background-color: #f0f0f0; font-weight: bold; }}
        .disclaimer {{ font-style: italic; font-size: 11px; margin-top: 25px; }}
    </style>
    </head>
    <body>
    <div class="container">
        <p>Estimados señores,</p>
        <p>Junto con saludar, me dirijo a ustedes. Con el fin de confirmar la recepción de la factura de su proveedor <strong>{emisor_razon_social} {emisor_rut}</strong>, está cediendo a Factoring Capital Express Servicios Financieros S.A. los siguientes documentos:</p>
        <ol>
            <li>¿Factura recepcionada con sus productos o servicios efectuados correctamente?</li>
            <li>¿Poseen Nota de Crédito?</li>
            <li>Fecha y forma de pago a Factoring.</li>
            <li>Correo electrónico de la persona a quién notificar.</li>
        </ol>
        <p><strong>Detalle de las facturas:</strong></p>
        {tabla_html}
        {correos_html}
        <p><strong>ID de operación:</strong> {operation_id}</p>
    </div>
    </body>
    </html>
    """

    return MIMEText(mensaje_html, "html")