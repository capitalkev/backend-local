"""
Modelos de base de datos (AJUSTADOS a la estructura REAL de la BD)
"""

from sqlalchemy import Column, Integer, String, DateTime, Numeric, Float
from datetime import datetime
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class Operacion(Base):
    __tablename__ = 'operaciones'

    # Columnas que SÍ existen en la BD
    id = Column(Integer, primary_key=True)
    operation_id = Column(String(100))
    usuario_id = Column(Integer, nullable=True)  # Es INTEGER, no String!
    emisor_rut = Column(String(50))
    emisor_razon_social = Column(String(255))
    total_monto = Column(Float)  # Es DOUBLE PRECISION
    tasa = Column(Numeric(10, 2))
    comision = Column(Integer)
    created_at = Column(DateTime)
    trello_card_id = Column(String(100), nullable=True)

class Factura(Base):
    __tablename__ = 'facturas'

    # Columnas que SÍ existen en la BD
    id = Column(Integer, primary_key=True)
    operacion_id = Column(Integer)  # Es INTEGER!
    tipo_dte = Column(String(10))
    folio = Column(Integer)
    monto_total = Column(Float)  # Es DOUBLE PRECISION
    fecha_emision = Column(String(20))
    receptor_rut = Column(String(50))
    receptor_razon_social = Column(String(255))
    pdf_filename = Column(String(255), nullable=True)
    source = Column(String(50))

class Gestion(Base):
    __tablename__ = 'gestiones'
    id = Column(Integer, primary_key=True, autoincrement=True)
    operation_id = Column(String(100))
    receptor_rut = Column(String(50))
    texto_gestion = Column(String(1000))
    usuario_email = Column(String(255))
    created_at = Column(DateTime, default=datetime.now)
