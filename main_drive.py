import os
import io
import traceback
import mimetypes
import threading
from typing import Optional
from fastapi import FastAPI, HTTPException, Request, BackgroundTasks
from google.cloud import storage
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from dotenv import load_dotenv
load_dotenv()
app = FastAPI(title="Drive Service (Thread-Safe)")

# --- Variables globales thread-safe ---
upload_progress = {}
upload_progress_lock = threading.Lock()

# --- Configuración ---
DRIVE_PARENT_FOLDER_ID = os.getenv("DRIVE_PARENT_FOLDER_ID", "1dl5FE6wKk6aXfspFrjm5YuS9rHP92Q_5")
SERVICE_ACCOUNT_FILE = 'service_account.json'
SCOPES = ['https://www.googleapis.com/auth/drive']

storage_client = storage.Client()

# Thread-local storage para drive service instances
drive_service_local = threading.local()

def get_drive_service():
    """
    Obtiene una instancia thread-safe de drive service.
    Cada thread tiene su propia instancia.
    """
    if not hasattr(drive_service_local, 'service'):
        try:
            creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)
            drive_service_local.service = build('drive', 'v3', credentials=creds)
            print(f"DRIVE: Nueva instancia de drive service creada para thread {threading.get_ident()}")
        except Exception as e:
            drive_service_local.service = None
            print(f"ADVERTENCIA: No se pudo inicializar el servicio de Drive para thread {threading.get_ident()}: {e}")
    
    return drive_service_local.service



def find_existing_folder(folder_name: str, parent_folder_id: str):
    """
    Busca una carpeta existente en Drive por nombre y padre (thread-safe)
    """
    try:
        drive_service = get_drive_service()
        if not drive_service:
            print("DRIVE: No hay servicio disponible para buscar carpeta")
            return None
            
        query = f"name='{folder_name}' and '{parent_folder_id}' in parents and mimeType='application/vnd.google-apps.folder' and trashed=false"
        results = drive_service.files().list(
            q=query,
            fields='files(id, name, webViewLink)',
            supportsAllDrives=True
        ).execute()
        
        files = results.get('files', [])
        if files:
            return files[0]
        return None
    except Exception as e:
        print(f"Error buscando carpeta existente: {e}")
        return None

def create_or_get_folder(folder_name: str, parent_folder_id: str):
    """
    Crea una carpeta o obtiene una existente si ya existe (thread-safe).
    Maneja la concurrencia donde múltiples procesos intentan crear la misma carpeta.
    """
    try:
        drive_service = get_drive_service()
        if not drive_service:
            raise Exception("No hay servicio Drive disponible")

        # 1. Primero verificar si ya existe
        existing_folder = find_existing_folder(folder_name, parent_folder_id)
        if existing_folder:
            print(f"DRIVE DIRECTO: Carpeta '{folder_name}' ya existe. URL: {existing_folder.get('webViewLink')}")
            return existing_folder

        # 2. Intentar crear nueva carpeta
        folder_metadata = {
            'name': folder_name, 
            'mimeType': 'application/vnd.google-apps.folder', 
            'parents': [parent_folder_id]
        }
        
        folder = drive_service.files().create(
            body=folder_metadata, 
            fields='id, webViewLink', 
            supportsAllDrives=True
        ).execute()
        
        # Verificar que folder es un diccionario válido
        if isinstance(folder, dict) and 'id' in folder:
            print(f"DRIVE DIRECTO: Carpeta '{folder_name}' creada exitosamente. URL: {folder.get('webViewLink')}")
            return folder
        else:
            # Si no es un diccionario válido, intentar buscar la carpeta que pudo haber sido creada por otro proceso
            print("DRIVE DIRECTO: Respuesta inesperada al crear carpeta, buscando carpeta existente...")
            existing_folder = find_existing_folder(folder_name, parent_folder_id)
            if existing_folder:
                return existing_folder
            else:
                raise Exception(f"No se pudo crear ni encontrar la carpeta '{folder_name}'")
                
    except Exception as e:
        print(f"DRIVE DIRECTO: Error creando carpeta '{folder_name}': {e}")
        # Intentar buscar carpeta existente como último recurso
        existing_folder = find_existing_folder(folder_name, parent_folder_id)
        if existing_folder:
            print("DRIVE DIRECTO: Usando carpeta existente tras error de creación")
            return existing_folder
        else:
            raise e

def upload_files_in_background(all_gcs_paths: list, folder_id: str, tracking_id: str):
    print(f"DRIVE-BG: Iniciando subida en segundo plano para {tracking_id}")
    
    # Inicializar progreso (thread-safe)
    with upload_progress_lock:
        upload_progress[tracking_id] = {
            "status": "uploading",
            "completed": 0,
            "total": len(all_gcs_paths),
            "failed": 0,
            "errors": []
        }
    
    # Obtener instancia thread-safe de drive service
    drive_service = get_drive_service()
    if not drive_service:
        with upload_progress_lock:
            upload_progress[tracking_id]["status"] = "failed"
            upload_progress[tracking_id]["errors"].append("No hay servicio Drive disponible")
        print(f"DRIVE-BG: Error - No hay servicio Drive disponible para {tracking_id}")
        return
    
    for i, gcs_path in enumerate(all_gcs_paths):
        try:
            bucket_name, blob_name = gcs_path.replace("gs://", "").split("/", 1)
            blob = storage_client.bucket(bucket_name).blob(blob_name)
            file_bytes = blob.download_as_bytes()
            mime_type, _ = mimetypes.guess_type(os.path.basename(gcs_path))
            mime_type = mime_type or 'application/octet-stream'

            file_metadata = {'name': os.path.basename(gcs_path), 'parents': [folder_id]}
            media = MediaIoBaseUpload(io.BytesIO(file_bytes), mimetype=mime_type, resumable=True)
            drive_service.files().create(body=file_metadata, media_body=media, fields='id', supportsAllDrives=True).execute()
            
            # Actualizar progreso exitoso (thread-safe)
            with upload_progress_lock:
                upload_progress[tracking_id]["completed"] = i + 1
            print(f"DRIVE-BG: Archivo {i+1}/{len(all_gcs_paths)} subido: {os.path.basename(gcs_path)}")
            
        except Exception as e:
            # Actualizar errores (thread-safe)
            with upload_progress_lock:
                upload_progress[tracking_id]["failed"] += 1
                upload_progress[tracking_id]["errors"].append(f"Error en {os.path.basename(gcs_path)}: {str(e)}")
            print(f"WARN-BG: Falló la subida de '{gcs_path}'. Error: {e}")
    
    # Marcar como completado (thread-safe)
    with upload_progress_lock:
        upload_progress[tracking_id]["status"] = "completed"
        completed = upload_progress[tracking_id]['completed']
        total = upload_progress[tracking_id]['total']
    
    print(f"DRIVE-BG: Subida en segundo plano para {tracking_id} completada. {completed}/{total} exitosos.")



@app.post("/archive-direct")
async def archive_direct(request: Request, background_tasks: BackgroundTasks):
    try:
        operation_data = await request.json()
        tracking_id = operation_data.get("tracking_id")
        operation_id = operation_data.get("operation_id")
        
        print(f"DRIVE DIRECTO: Procesando {tracking_id}")
        
        # Verificar disponibilidad del servicio Drive (thread-safe)
        drive_service = get_drive_service()
        if not drive_service:
            raise HTTPException(status_code=503, detail="Servicio de Drive no disponible")

        if not operation_id:
            print(f"ERROR: operation_id no encontrado para tracking_id: {tracking_id}")
            raise HTTPException(status_code=400, detail="El 'operation_id' es requerido para crear la carpeta Drive.")
        
        # Crear o obtener carpeta existente (manejo de concurrencia)
        folder_name = f"Operacion_{operation_id}"
        print(f"DRIVE DIRECTO: Creando/obteniendo carpeta '{folder_name}' para operación {operation_id}")
        
        folder = create_or_get_folder(folder_name, DRIVE_PARENT_FOLDER_ID)
        
        folder_id = folder.get('id')
        folder_url = folder.get('webViewLink')
        
        gcs_paths = operation_data.get("gcs_paths", {})
        all_gcs_paths = gcs_paths.get('xml', []) + gcs_paths.get('pdf', []) + gcs_paths.get('respaldo', [])
        
        if all_gcs_paths:
            background_tasks.add_task(upload_files_in_background, all_gcs_paths, folder_id, tracking_id)
            print(f"DRIVE DIRECTO: {len(all_gcs_paths)} archivos programados para subida en background")
        
        return {"drive_folder_url": folder_url}
        
    except Exception as e:
        print(f"DRIVE DIRECTO: Error para {tracking_id}: {e}")
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/upload-progress/{tracking_id}")
async def get_upload_progress(tracking_id: str):
    """Obtiene el progreso de subida para un tracking_id específico (thread-safe)"""
    with upload_progress_lock:
        progress = upload_progress.get(tracking_id, {"status": "not_found"})
    return progress


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    # Llama a la función para obtener el servicio
    drive_service_instance = get_drive_service()
    return {
        "status": "healthy",
        # Usa la variable que acabas de crear
        "drive_service": "available" if drive_service_instance else "unavailable",
        "active_uploads": len([t for t, p in upload_progress.items() if p.get("status") == "uploading"])
    }