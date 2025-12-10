"""
Funciones CRUD
"""

from sqlalchemy.orm import Session
from sqlalchemy import text
from models import Operacion, Gestion
from typing import Optional


def listar_operaciones(db: Session):
    """
    Trae solo la tabla operaciones, sin JOINS pesados.
    """
    sql = """
        SELECT 
            operation_id,
            emisor_razon_social,
            emisor_rut,
            total_monto,
            usuario_id
        FROM operaciones
        ORDER BY created_at DESC
    """
    result = db.execute(text(sql))
    return [dict(row._mapping) for row in result]


def obtener_detalles_operacion(db: Session, op_id: str):
    """
    Trae las facturas SOLO de la operación solicitada
    """
    sql = """
        SELECT f.* FROM facturas f
        JOIN operaciones o ON f.operacion_id = o.operation_id
        WHERE o.operation_id = :op_id
    """
    params = {"op_id": op_id}
    result = db.execute(text(sql), params)
    return [dict(row._mapping) for row in result]


def obtener_operacion_por_id(db: Session, op_id: str) -> Optional[Operacion]:
    """
    Obtiene una operación por su ID.
    """
    return db.query(Operacion).filter(Operacion.operation_id == op_id).first()


def obtener_contactos(db: Session, rut_receptor: str):
    """
    Obtiene los contactos asociados a un receptor específico.
    """
    sql = """
        SELECT DISTINCT(email) FROM contactos
        WHERE rut = :rut_receptor
    """
    params = {"rut_receptor": rut_receptor}
    result = db.execute(text(sql), params)
    return [dict(row._mapping) for row in result]


def obtener_email_filtrados(db: Session, ruc_cliente: str, ruc_deudor: str):
    """
    Obtiene los emails filtrados para un cliente y deudor específicos.
    """
    sql = """
        SELECT DISTINCT(email) FROM db_filtrado
        WHERE ruc_cliente = :ruc_cliente AND ruc_deudor = :ruc_deudor
    """
    params = {
        "ruc_cliente": ruc_cliente,
        "ruc_deudor": ruc_deudor,
    }
    result = db.execute(text(sql), params)
    return [dict(row._mapping) for row in result]

def agregar_contactos(db: Session, ruc_cliente: str, emails: list, ruc_deudor: str):
    try:
        for email in emails:
            sql = """
                INSERT INTO db_filtrado (ruc_cliente, email, ruc_deudor)
                VALUES (:ruc_cliente, :email, :ruc_deudor)
            """
            params = {
                "ruc_cliente": ruc_cliente,
                "email": email,
                "ruc_deudor": ruc_deudor,
            }
            db.execute(text(sql), params)

        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e
