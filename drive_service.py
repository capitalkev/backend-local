"""
Servicio de Google Drive - Descarga de archivos desde carpetas de Drive
Basado en main_drive.py pero enfocado en DESCARGAR archivos
"""

import os
import io
import re
import tempfile
import logging
from typing import List, Optional, Dict
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.errors import HttpError

# Configuración
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive.readonly']  # Solo lectura para descargar

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def get_drive_service():
    """
    Crea y retorna una instancia del servicio de Google Drive API
    usando autenticación con service account (similar a main_drive.py líneas 30-44)

    Returns:
        Resource: Servicio de Google Drive API autenticado
        None: Si hay error en la autenticación
    """
    try:
        if not os.path.exists(SERVICE_ACCOUNT_FILE):
            logger.error(f"Archivo de credenciales no encontrado: {SERVICE_ACCOUNT_FILE}")
            return None

        creds = Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=SCOPES
        )
        service = build('drive', 'v3', credentials=creds)
        logger.info("Servicio de Google Drive creado exitosamente")
        return service
    except Exception as e:
        logger.error(f"Error al crear servicio de Drive: {e}")
        return None


def extraer_folder_id_desde_url(drive_url: str) -> Optional[str]:
    """
    Extrae el ID de la carpeta desde una URL de Google Drive

    Args:
        drive_url: URL completa de la carpeta
                  Ej: https://drive.google.com/drive/folders/1dl5FE6wKk6aXfspFrjm5YuS9rHP92Q_5

    Returns:
        str: ID de la carpeta (ej: "1dl5FE6wKk6aXfspFrjm5YuS9rHP92Q_5")
        None: Si no se puede extraer el ID
    """
    try:
        # Patrón para extraer el ID de diferentes formatos de URL de Drive
        patterns = [
            r'folders/([a-zA-Z0-9_-]+)',  # /folders/ID
            r'id=([a-zA-Z0-9_-]+)',        # ?id=ID
            r'^([a-zA-Z0-9_-]+)$'          # Solo el ID
        ]

        for pattern in patterns:
            match = re.search(pattern, drive_url)
            if match:
                folder_id = match.group(1)
                logger.info(f"Folder ID extraído: {folder_id}")
                return folder_id

        logger.warning(f"No se pudo extraer folder ID de: {drive_url}")
        return None
    except Exception as e:
        logger.error(f"Error al extraer folder ID: {e}")
        return None


def listar_archivos_en_carpeta(
    drive_service,
    folder_id: str,
    mime_type: Optional[str] = None
) -> List[Dict]:
    """
    Lista todos los archivos en una carpeta de Drive
    Similar a find_existing_folder de main_drive.py pero para listar archivos

    Args:
        drive_service: Servicio de Google Drive API
        folder_id: ID de la carpeta de Drive
        mime_type: Filtro opcional por tipo MIME (ej: 'application/pdf')

    Returns:
        List[Dict]: Lista de archivos con {'id', 'name', 'mimeType', 'size'}
    """
    try:
        # Construir query para buscar archivos en la carpeta
        query = f"'{folder_id}' in parents and trashed=false"

        if mime_type:
            query += f" and mimeType='{mime_type}'"

        logger.info(f"Listando archivos en carpeta {folder_id}")

        results = drive_service.files().list(
            q=query,
            fields='files(id, name, mimeType, size)',
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,  # Incluir archivos de Drives compartidos
            pageSize=1000  # Máximo por página
        ).execute()

        archivos = results.get('files', [])
        logger.info(f"Encontrados {len(archivos)} archivos en la carpeta")

        return archivos

    except HttpError as error:
        logger.error(f"Error HTTP al listar archivos: {error}")
        return []
    except Exception as e:
        logger.error(f"Error al listar archivos: {e}")
        return []


def descargar_archivo_desde_drive(
    drive_service,
    file_id: str,
    file_name: str,
    output_dir: Optional[str] = None
) -> Optional[str]:
    """
    Descarga un archivo desde Google Drive a disco local

    Args:
        drive_service: Servicio de Google Drive API
        file_id: ID del archivo en Drive
        file_name: Nombre del archivo
        output_dir: Directorio de salida (opcional, usa temp si no se especifica)

    Returns:
        str: Ruta completa del archivo descargado
        None: Si hay error en la descarga
    """
    try:
        # Usar directorio temporal si no se especifica
        if output_dir is None:
            output_dir = tempfile.gettempdir()

        # Crear directorio si no existe
        os.makedirs(output_dir, exist_ok=True)

        # Ruta completa del archivo de salida
        output_path = os.path.join(output_dir, file_name)

        logger.info(f"Descargando archivo: {file_name} (ID: {file_id})")

        # Solicitar el archivo desde Drive
        request = drive_service.files().get_media(fileId=file_id)

        # Descargar en chunks
        with io.FileIO(output_path, 'wb') as fh:
            downloader = MediaIoBaseDownload(fh, request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    logger.debug(f"Descarga {int(status.progress() * 100)}% completada")

        logger.info(f"Archivo descargado exitosamente: {output_path}")
        return output_path

    except HttpError as error:
        logger.error(f"Error HTTP al descargar archivo {file_name}: {error}")
        return None
    except Exception as e:
        logger.error(f"Error al descargar archivo {file_name}: {e}")
        return None


def descargar_todos_archivos_de_carpeta(
    folder_url: str,
    mime_type: Optional[str] = 'application/pdf',
    output_dir: Optional[str] = None
) -> List[str]:
    """
    Función principal: Descarga todos los archivos de una carpeta de Drive

    Args:
        folder_url: URL completa de la carpeta de Drive
        mime_type: Tipo MIME a filtrar (default: PDF)
        output_dir: Directorio de salida (opcional, usa temp si no se especifica)

    Returns:
        List[str]: Lista de rutas locales de archivos descargados
    """
    archivos_descargados = []

    try:
        # 1. Autenticar con Drive
        drive_service = get_drive_service()
        if not drive_service:
            logger.error("No se pudo crear servicio de Drive")
            return archivos_descargados

        # 2. Extraer folder ID de la URL
        folder_id = extraer_folder_id_desde_url(folder_url)
        if not folder_id:
            logger.error(f"No se pudo extraer folder ID de: {folder_url}")
            return archivos_descargados

        # 3. Listar archivos en la carpeta
        archivos = listar_archivos_en_carpeta(drive_service, folder_id, mime_type)

        if not archivos:
            logger.warning(f"No se encontraron archivos en la carpeta {folder_id}")
            return archivos_descargados

        # 4. Descargar cada archivo
        logger.info(f"Iniciando descarga de {len(archivos)} archivos...")

        for archivo in archivos:
            file_id = archivo.get('id')
            file_name = archivo.get('name')

            if not file_id or not file_name:
                logger.warning(f"Archivo sin ID o nombre: {archivo}")
                continue

            ruta_local = descargar_archivo_desde_drive(
                drive_service,
                file_id,
                file_name,
                output_dir
            )

            if ruta_local:
                archivos_descargados.append(ruta_local)

        logger.info(f"Descarga completada: {len(archivos_descargados)}/{len(archivos)} archivos exitosos")
        return archivos_descargados

    except Exception as e:
        logger.error(f"Error general al descargar archivos: {e}")
        return archivos_descargados


def limpiar_archivos_temporales(rutas_archivos: List[str]) -> None:
    """
    Elimina archivos temporales después de su uso

    Args:
        rutas_archivos: Lista de rutas de archivos a eliminar
    """
    for ruta in rutas_archivos:
        try:
            if os.path.exists(ruta):
                os.remove(ruta)
                logger.debug(f"Archivo temporal eliminado: {ruta}")
        except Exception as e:
            logger.warning(f"No se pudo eliminar archivo {ruta}: {e}")
