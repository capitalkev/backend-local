"""
Database Configuration - PostgreSQL Connection usando Cloud SQL Python Connector
"""

import os
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from dotenv import load_dotenv

load_dotenv()

# Configurar credenciales de Google Cloud
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
if GOOGLE_APPLICATION_CREDENTIALS:
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = GOOGLE_APPLICATION_CREDENTIALS

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")

# Determinar si usamos Cloud SQL Connector o conexión TCP local
if INSTANCE_CONNECTION_NAME:
    # Conexión a Cloud SQL usando Python Connector
    from google.cloud.sql.connector import Connector

    connector = Connector()

    def getconn():
        conn = connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=DB_USER,
            password=DB_PASSWORD,
            db=DB_NAME
        )
        return conn

    engine = create_engine(
        "postgresql+pg8000://",
        creator=getconn,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    print(f"[DATABASE] Usando Cloud SQL Connector: {INSTANCE_CONNECTION_NAME}")
else:
    # Conexión local TCP
    DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(DATABASE_URL, echo=False)
    print(f"[DATABASE] Usando PostgreSQL local: {DB_HOST}:{DB_PORT}")

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

def get_db():
    """Dependency para obtener sesión de base de datos"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def test_connection():
    """Prueba la conexión a PostgreSQL"""
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version = result.fetchone()[0]
            print(f"[OK] PostgreSQL conectado: {version}")
            return True
    except Exception as e:
        print(f"[ERROR] Error conectando a PostgreSQL: {e}")
        return False
