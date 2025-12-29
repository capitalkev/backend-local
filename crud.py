from sqlalchemy.orm import Session
from sqlalchemy import text
from models import Operacion
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
    """
    Agrega emails a db_filtrado, evitando duplicados.
    Retorna la cantidad de emails agregados y duplicados.
    """
    try:
        emails_agregados = []
        emails_duplicados = []

        for email in emails:
            # Verificar si el email ya existe para este cliente-deudor
            check_sql = """
                SELECT COUNT(*) as count FROM db_filtrado
                WHERE ruc_cliente = :ruc_cliente
                AND ruc_deudor = :ruc_deudor
                AND email = :email
            """
            check_params = {
                "ruc_cliente": ruc_cliente,
                "ruc_deudor": ruc_deudor,
                "email": email,
            }
            result = db.execute(text(check_sql), check_params).fetchone()

            if result[0] == 0:  # No existe, insertar
                insert_sql = """
                    INSERT INTO db_filtrado (ruc_cliente, email, ruc_deudor)
                    VALUES (:ruc_cliente, :email, :ruc_deudor)
                """
                db.execute(text(insert_sql), check_params)
                emails_agregados.append(email)
            else:
                emails_duplicados.append(email)

        db.commit()
        return {"agregados": emails_agregados, "duplicados": emails_duplicados}
    except Exception as e:
        db.rollback()
        raise e


def obtener_contactos_sugeridos(db: Session, ruc_cliente: str, ruc_deudor: str):
    """
    Devuelve los emails de contactos que NO están en db_filtrado.
    Estos son los emails disponibles para agregar.
    """
    sql = """  
        SELECT email FROM db_filtrado
        WHERE ruc_cliente = :ruc_cliente AND ruc_deudor = :ruc_deudor
        EXCEPT
        SELECT DISTINCT email FROM contactos
        WHERE rut = :ruc_deudor
    """
    params = {
        "ruc_cliente": ruc_cliente,
        "ruc_deudor": ruc_deudor,
    }
    result = db.execute(text(sql), params)
    return [dict(row._mapping) for row in result]


def editar_contacto(
    db: Session, ruc_cliente: str, ruc_deudor: str, email_viejo: str, email_nuevo: str
):
    """
    Actualiza un email en db_filtrado.
    """
    try:
        # Verificar que el email_viejo existe
        check_sql = """
            SELECT COUNT(*) as count FROM db_filtrado
            WHERE ruc_cliente = :ruc_cliente
            AND ruc_deudor = :ruc_deudor
            AND email = :email_viejo
        """
        check_params = {
            "ruc_cliente": ruc_cliente,
            "ruc_deudor": ruc_deudor,
            "email_viejo": email_viejo,
        }
        result = db.execute(text(check_sql), check_params).fetchone()

        if result[0] == 0:
            raise ValueError(f"Email '{email_viejo}' no encontrado")

        # Verificar que el email_nuevo no existe ya
        check_nuevo_sql = """
            SELECT COUNT(*) as count FROM db_filtrado
            WHERE ruc_cliente = :ruc_cliente
            AND ruc_deudor = :ruc_deudor
            AND email = :email_nuevo
        """
        check_nuevo_params = {
            "ruc_cliente": ruc_cliente,
            "ruc_deudor": ruc_deudor,
            "email_nuevo": email_nuevo,
        }
        result_nuevo = db.execute(text(check_nuevo_sql), check_nuevo_params).fetchone()

        if result_nuevo[0] > 0:
            raise ValueError(f"Email '{email_nuevo}' ya existe")

        # Actualizar el email
        update_sql = """
            UPDATE db_filtrado
            SET email = :email_nuevo
            WHERE ruc_cliente = :ruc_cliente
            AND ruc_deudor = :ruc_deudor
            AND email = :email_viejo
        """
        update_params = {
            "ruc_cliente": ruc_cliente,
            "ruc_deudor": ruc_deudor,
            "email_viejo": email_viejo,
            "email_nuevo": email_nuevo,
        }
        db.execute(text(update_sql), update_params)
        db.commit()
        return True
    except Exception as e:
        db.rollback()
        raise e


def eliminar_contacto(db: Session, ruc_cliente: str, ruc_deudor: str, email: str):
    """
    Elimina un email de db_filtrado.
    """
    try:
        sql = """
            DELETE FROM db_filtrado
            WHERE ruc_cliente = :ruc_cliente
            AND ruc_deudor = :ruc_deudor
            AND email = :email
        """
        params = {
            "ruc_cliente": ruc_cliente,
            "ruc_deudor": ruc_deudor,
            "email": email,
        }
        result = db.execute(text(sql), params)
        db.commit()

        if result.rowcount == 0:
            raise ValueError(f"Email '{email}' no encontrado")

        return True
    except Exception as e:
        db.rollback()
        raise e


def extraer_drive(db: Session, operacion_id: str):
    """
    Extrae archivos de Google Drive.
    """

    sql = """
        SELECT drive_folder_url FROM operaciones
        WHERE operation_id = :operation_id
    """
    params = {"operation_id": operacion_id}
    result = db.execute(text(sql), params).fetchone()
    if result:
        return dict(result._mapping)
    return None


def obtener_operacion_completa(db: Session, op_id: str):
    """
    Obtiene una operación completa con sus deudores y facturas.
    Retorna la misma estructura que el endpoint /operaciones/{op_id}
    """
    # 1. Obtener las facturas específicas de esta operación
    facturas = obtener_detalles_operacion(db, op_id)
    operacion = obtener_operacion_por_id(db, op_id)

    if not facturas:
        return None

    deudores_dict = {}

    for factura in facturas:
        rut = factura["receptor_rut"] or ""

        if rut not in deudores_dict:
            deudores_dict[rut] = {
                "nombre": factura["receptor_razon_social"] or "Sin nombre",
                "ruc": rut.split("-")[0].replace(".", "").strip(),
                "facturas": [],
                "gestiones": [],
                "contactos": [],
            }

        deudores_dict[rut]["facturas"].append(
            {
                "folio": str(factura["folio"]),
                "tipoDTE": factura["tipo_dte"] or "33",
                "montoFactura": float(factura["monto_total"])
                if factura["monto_total"]
                else 0,
                "fechaEmision": factura["fecha_emision"] or "",
                "estado": "Pendiente",
            }
        )

    return {
        "operacion": {
            "id": operacion.id,
            "operation_id": operacion.operation_id,
            "emisor_razon_social": operacion.emisor_razon_social,
            "emisor_rut": operacion.emisor_rut,
            "total_monto": float(operacion.total_monto) if operacion.total_monto else 0,
            "tasa": float(operacion.tasa) if operacion.tasa else 0,
            "comision": operacion.comision,
            "created_at": operacion.created_at.isoformat()
            if operacion.created_at
            else None,
            "usuario_id": operacion.usuario_id,
            "trello_card_id": operacion.trello_card_id,
            "drive_folder_url": operacion.drive_folder_url,
        },
        "deudores": list(deudores_dict.values()),
    }
