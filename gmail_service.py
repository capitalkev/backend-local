import os.path
import mimetypes
import logging
import requests
from typing import Optional, Dict, List
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email import encoders
from datetime import datetime
from fastapi import Depends
from sqlalchemy.orm import Session
from sqlalchemy import text

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

from drive_service import (
    descargar_todos_archivos_de_carpeta,
    limpiar_archivos_temporales,
)

SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
PDF_SERVICE_URL = "http://localhost:8001"  # URL del servicio de procesamiento de PDFs

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def procesar_pdf_y_extraer_rut(pdf_path: str) -> Optional[str]:
    """
    Procesa un PDF usando pdf_service para extraer el RUT del receptor.

    Args:
        pdf_path: Ruta local del archivo PDF

    Returns:
        str: RUT del receptor (sin puntos, con guión) o None si hay error
    """
    try:
        # Llamar al servicio de procesamiento de PDFs
        with open(pdf_path, 'rb') as pdf_file:
            files = {'file': (os.path.basename(pdf_path), pdf_file, 'application/pdf')}
            response = requests.post(
                f"{PDF_SERVICE_URL}/procesar-pdf/",
                files=files,
                timeout=30
            )

        if response.status_code == 200:
            data = response.json()
            if data.get('success'):
                # Extraer RUT del receptor
                receptor_data = data.get('data', {}).get('receptor', {})
                rut_receptor = receptor_data.get('rut')

                if rut_receptor:
                    # Normalizar el RUT (quitar puntos, dejar solo guión)
                    rut_normalizado = rut_receptor.replace('.', '').strip()
                    logger.info(f"RUT extraído de {os.path.basename(pdf_path)}: {rut_normalizado}")
                    return rut_normalizado
                else:
                    logger.warning(f"No se encontró RUT del receptor en {os.path.basename(pdf_path)}")
            else:
                logger.warning(f"pdf_service no pudo procesar {os.path.basename(pdf_path)}")
        else:
            logger.error(f"Error al llamar a pdf_service: {response.status_code}")

    except requests.exceptions.RequestException as e:
        logger.error(f"Error de conexión con pdf_service: {e}")
    except Exception as e:
        logger.error(f"Error al procesar PDF {pdf_path}: {e}")

    return None


def obtener_rut_deudor_desde_email(db: Session, email: str, emisor_rut: str) -> Optional[str]:
    """
    Obtiene el RUT del deudor a partir de un email usando la tabla db_filtrado.

    Args:
        db: Sesión de base de datos
        email: Email del contacto del deudor
        emisor_rut: RUT del emisor (cliente) para filtrar

    Returns:
        str: RUT del deudor (sin puntos, sin guión) o None si no se encuentra
    """
    try:
        # Normalizar el RUT del emisor (quitar puntos y guión)
        emisor_rut_limpio = emisor_rut.replace('.', '').replace('-', '').strip()

        sql = """
            SELECT ruc_deudor FROM db_filtrado
            WHERE email = :email AND ruc_cliente = :ruc_cliente
            LIMIT 1
        """
        params = {
            "email": email.strip(),
            "ruc_cliente": emisor_rut_limpio
        }
        result = db.execute(text(sql), params).fetchone()

        if result:
            rut_deudor = result[0]
            logger.info(f"RUT del deudor para email {email}: {rut_deudor}")
            return rut_deudor
        else:
            logger.warning(f"No se encontró RUT del deudor para email {email}")
            return None

    except Exception as e:
        logger.error(f"Error al obtener RUT del deudor desde email: {e}")
        return None


def normalizar_rut(rut: str) -> str:
    """
    Normaliza un RUT para comparación (quita puntos y dígito verificador).
    Solo deja el número del RUT sin el dígito verificador.

    Args:
        rut: RUT en cualquier formato (ej: "12.345.678-9", "12345678-9", "12345678")

    Returns:
        str: RUT sin puntos, sin guión, sin dígito verificador (ej: "12345678")
    """
    # Quitar puntos
    rut_limpio = rut.replace('.', '').strip()

    # Si tiene guión, tomar solo la parte antes del guión (quitar dígito verificador)
    if '-' in rut_limpio:
        rut_limpio = rut_limpio.split('-')[0]

    return rut_limpio.upper()


def autenticar_gmail():
    """
    Autentica y crea el servicio de Gmail API

    Returns:
        Resource: Servicio de Gmail API autenticado
    """
    if os.path.exists("token.json"):
        creds = Credentials.from_authorized_user_file("token.json", SCOPES)

    return build("gmail", "v1", credentials=creds)


def enviar_correo_multiples(
    servicio,
    destinatarios: str,
    op_id: str,
    db: Session = Depends(get_db),
    drive_url: str = None,
    rut_deudor: str = None,
) -> Optional[Dict]:
    """
    Envía un correo electrónico a uno o múltiples destinatarios usando Gmail API

    Args:
        servicio: Servicio de Gmail API
        destinatarios: Emails separados por comas (ej: "email1@test.com,email2@test.com")
        op_id: ID de la operación
        db: Sesión de base de datos
        drive_url: URL de la carpeta de Drive
        rut_deudor: RUT del deudor para filtrar PDFs (sin puntos, con o sin guión)

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

        datos_completos = obtener_operacion_completa(db, op_id)
        if not datos_completos:
            logger.error(f"No se encontró la operación con ID: {op_id}")
            return None

        # Obtener nombre del emisor para el asunto
        operacion = datos_completos.get("operacion", {})
        emisor_nombre = operacion.get("emisor_razon_social", "N/A")

        mensaje = MIMEMultipart()
        mensaje["to"] = ", ".join(lista_destinatarios)
        mensaje["from"] = USER_ID
        mensaje["subject"] = f"Confirmación de Cesión de Facturas - {emisor_nombre}"

        cuerpo = cuerpo_template(datos_completos, destinatarios)
        mensaje.attach(cuerpo)

        # Descargar y adjuntar archivos desde Google Drive (filtrados por RUT del deudor)
        archivos_descargados = []
        if drive_url and drive_url.get("drive_folder_url"):
            folder_url = drive_url.get("drive_folder_url")
            logger.info(f"Descargando archivos desde Drive: {folder_url}")

            try:
                # 1. Verificar que se proporcionó el RUT del deudor
                if not rut_deudor:
                    logger.warning("No se proporcionó RUT del deudor")
                    logger.info("Enviando correo sin adjuntos debido a RUT no proporcionado")
                else:
                    logger.info(f"RUT del deudor destinatario: {rut_deudor}")

                    # 2. Descargar todos los PDFs de la carpeta Drive
                    archivos_descargados = descargar_todos_archivos_de_carpeta(
                        folder_url=folder_url,
                        mime_type='application/pdf',  # Solo PDFs
                        output_dir=None  # Usa directorio temporal
                    )

                    if archivos_descargados:
                        logger.info(f"Descargados {len(archivos_descargados)} archivos desde Drive")

                        # 3. Procesar cada PDF y filtrar por RUT del deudor
                        archivos_a_adjuntar = []
                        for archivo_path in archivos_descargados:
                            try:
                                # Extraer RUT del receptor del PDF
                                rut_receptor_pdf = procesar_pdf_y_extraer_rut(archivo_path)

                                if rut_receptor_pdf:
                                    # Normalizar ambos RUTs para comparación
                                    rut_receptor_norm = normalizar_rut(rut_receptor_pdf)
                                    rut_deudor_norm = normalizar_rut(rut_deudor)

                                    # Comparar RUTs
                                    if rut_receptor_norm == rut_deudor_norm:
                                        archivos_a_adjuntar.append(archivo_path)
                                        logger.info(f"✓ PDF coincide con deudor: {os.path.basename(archivo_path)}")
                                    else:
                                        logger.info(f"✗ PDF NO coincide (Receptor: {rut_receptor_norm}, Deudor: {rut_deudor_norm}): {os.path.basename(archivo_path)}")
                                else:
                                    logger.warning(f"No se pudo extraer RUT de: {os.path.basename(archivo_path)}")

                            except Exception as e:
                                logger.error(f"Error al procesar PDF {archivo_path}: {e}")

                        # 4. Adjuntar solo los PDFs que coinciden con el RUT del deudor
                        if archivos_a_adjuntar:
                            logger.info(f"Adjuntando {len(archivos_a_adjuntar)} de {len(archivos_descargados)} PDFs al correo")

                            for archivo_path in archivos_a_adjuntar:
                                try:
                                    ctype, encoding = mimetypes.guess_type(archivo_path)
                                    if ctype is None or encoding is not None:
                                        ctype = "application/octet-stream"
                                    maintype, subtype = ctype.split("/", 1)

                                    with open(archivo_path, "rb") as fp:
                                        msg_adjunto = MIMEBase(maintype, subtype)
                                        msg_adjunto.set_payload(fp.read())

                                    encoders.encode_base64(msg_adjunto)
                                    filename = os.path.basename(archivo_path)
                                    msg_adjunto.add_header("Content-Disposition", "attachment", filename=filename)
                                    mensaje.attach(msg_adjunto)
                                    logger.info(f"Adjuntado: {filename}")

                                except Exception as e:
                                    logger.warning(f"Error al adjuntar archivo {archivo_path}: {e}")
                        else:
                            logger.warning(f"No se encontraron PDFs que coincidan con el RUT del deudor {rut_deudor}")

                    else:
                        logger.warning("No se encontraron archivos para descargar desde Drive")

            except Exception as e:
                logger.error(f"Error al procesar archivos de Drive: {e}")
                # Continuar con el envío aunque falle la descarga de archivos
        else:
            logger.info("No se proporcionó URL de Drive, enviando correo sin adjuntos")

        # Enviar el correo
        resultado = enviar_mensaje_gmail(servicio, mensaje)

        # Limpiar archivos temporales después de enviar
        if archivos_descargados:
            try:
                limpiar_archivos_temporales(archivos_descargados)
                logger.info(f"Archivos temporales limpiados: {len(archivos_descargados)}")
            except Exception as e:
                logger.warning(f"Error al limpiar archivos temporales: {e}")

        if resultado:
            mensaje_id = resultado["id"]
            thread_id = resultado.get("threadId")
            logger.info(
                f"Correo enviado a {len(lista_destinatarios)} destinatario(s). ID: {mensaje_id}"
            )
            return {
                "mensaje_id": mensaje_id,
                "destinatarios": lista_destinatarios,
                "thread_id": thread_id,
            }

        logger.warning("No se pudo enviar el correo")
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
        asunto_original = extraer_header(headers, "Subject")  #

        if not asunto_original:
            logger.warning(
                "ALERTA: No se encontró asunto original. El hilo podría romperse."
            )
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
                nuevo_mensaje["References"] = (
                    f"{references_original} {message_id_original}"
                )
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
        if "." in formatted:
            parts = formatted.split(".")
            integer_part = parts[0].replace(",", ".")
            decimal_part = parts[1]
            return f"{integer_part},{decimal_part}"
        else:
            return formatted.replace(",", ".")
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
    if not date_str or date_str == "N/A":
        return "N/A"

    try:
        # Mapeo de meses en español (abreviado, minúsculas)
        meses = {
            1: "ene",
            2: "feb",
            3: "mar",
            4: "abr",
            5: "may",
            6: "jun",
            7: "jul",
            8: "ago",
            9: "sep",
            10: "oct",
            11: "nov",
            12: "dic",
        }

        # Parsear la fecha en formato YYYY-MM-DD
        fecha = datetime.strptime(date_str, "%Y-%m-%d")

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
    lista_destinatarios = [
        email.strip() for email in destinatarios.split(",") if email.strip()
    ]
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
