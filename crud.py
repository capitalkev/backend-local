"""
Funciones CRUD (AJUSTADAS a la estructura real de la BD)
"""
from sqlalchemy.orm import Session
from sqlalchemy import text
from models import Operacion, Factura, Gestion
from datetime import datetime, timedelta
from typing import Optional


def listar_operaciones(db: Session, usuario_email: Optional[str] = None, dias: Optional[int] = None, limit: Optional[int] = None):
    """
    Lista operaciones usando SQL directo para evitar joins automáticos
    """
    sql = """
        SELECT
            id,
            operation_id,
            usuario_id,
            emisor_rut,
            emisor_razon_social,
            total_monto,
            tasa,
            comision,
            created_at,
            trello_card_id
        FROM operaciones
        ORDER BY created_at DESC
    """

    params = {}

    if limit:
        sql += " LIMIT :limit"
        params["limit"] = limit

    result = db.execute(text(sql), params)
    rows = result.fetchall()

    # Convertir rows a objetos tipo Operacion
    operaciones = []
    for row in rows:
        op = Operacion()
        op.id = row[0]
        op.operation_id = row[1]
        op.usuario_id = row[2]
        op.emisor_rut = row[3]
        op.emisor_razon_social = row[4]
        op.total_monto = row[5]
        op.tasa = row[6]
        op.comision = row[7]
        op.created_at = row[8]
        op.trello_card_id = row[9]
        operaciones.append(op)

    return operaciones


def obtener_facturas_por_operation_id(db: Session, operation_id: str):
    """
    Obtiene facturas por operation_id (necesita JOIN manual porque los tipos no coinciden)
    """
    # Consulta SQL directa porque operation_id es VARCHAR en operaciones pero INTEGER en facturas
    query = text("""
        SELECT f.*
        FROM facturas f
        INNER JOIN operaciones o ON f.operacion_id = o.id
        WHERE o.operation_id = :operation_id
    """)

    result = db.execute(query, {"operation_id": operation_id})
    return result.fetchall()


def listar_gestiones_por_deudor(db: Session, operation_id: str, receptor_rut: str):
    """Lista todas las gestiones de un deudor específico en una operación"""
    return db.query(Gestion).filter(
        Gestion.operation_id == operation_id,
        Gestion.receptor_rut == receptor_rut
    ).order_by(Gestion.created_at.desc()).all()
