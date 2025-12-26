"""
Test de conexión a Cloud SQL usando Python Connector
"""
import os
from dotenv import load_dotenv
from google.cloud.sql.connector import Connector

load_dotenv()

# Configurar credenciales
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS
    print(f"[CONFIG] Usando credenciales: {GOOGLE_APPLICATION_CREDENTIALS}")

INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

print(f"[CONFIG] Instancia: {INSTANCE_CONNECTION_NAME}")
print(f"[CONFIG] Usuario: {DB_USER}")
print(f"[CONFIG] Base de datos: {DB_NAME}")
print("\n[TEST] Intentando conectar a Cloud SQL...")

try:
    connector = Connector()

    conn = connector.connect(
        INSTANCE_CONNECTION_NAME,
        "pg8000",
        user=DB_USER,
        password=DB_PASSWORD,
        db=DB_NAME
    )

    print("[OK] Conexión exitosa!")

    # Ejecutar query de prueba
    cursor = conn.cursor()
    cursor.execute("SELECT version();")
    version = cursor.fetchone()
    print(f"[OK] PostgreSQL version: {version[0]}")

    # Probar query a la tabla operaciones
    cursor.execute("SELECT COUNT(*) FROM operaciones;")
    count = cursor.fetchone()
    print(f"[OK] Total de operaciones en BD: {count[0]}")

    cursor.close()
    conn.close()
    connector.close()

    print("\n[SUCCESS] Test completado exitosamente!")

except Exception as e:
    print(f"\n[ERROR] Error en conexión: {e}")
    import traceback
    traceback.print_exc()
