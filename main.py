"""
Backend Local Simple - Sin Autenticación
Puerto: 8002
Solo para desarrollo local con frontend-local
"""

import uvicorn
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session

from database import get_db, test_connection
from crud import listar_operaciones, listar_gestiones_por_deudor
from models import Factura

app = FastAPI(
    title="Backend Local - Operaciones",
    version="1.0.0",
    description="Backend simple sin autenticación para frontend-local"
)

# Configurar CORS (permite todos los orígenes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Verifica la conexión a la base de datos al iniciar"""
    print("[STARTUP] Iniciando Backend Local...")
    test_connection()


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "Backend Local - Operaciones",
        "version": "1.0.0",
        "description": "Backend simple sin autenticación para desarrollo local",
        "endpoints": {
            "operaciones": "GET /operaciones/ - Lista todas las operaciones"
        }
    }


@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "service": "backend-local"}


@app.get("/operaciones/")
async def listar_operaciones_endpoint(db: Session = Depends(get_db)):
    """
    Lista todas las operaciones en formato compatible con frontend-local
    """
    try:
        operaciones = listar_operaciones(db, usuario_email=None, dias=None, limit=None)

        # Obtener todas las facturas para agruparlas
        todas_facturas = db.query(Factura).all()

        # DEBUG
        print(f"[DEBUG] Total operaciones: {len(operaciones)}")
        print(f"[DEBUG] Total facturas: {len(todas_facturas)}")
        if todas_facturas:
            print(f"[DEBUG] Primera factura operacion_id: {todas_facturas[0].operacion_id} (tipo: {type(todas_facturas[0].operacion_id)})")
        if operaciones:
            print(f"[DEBUG] Primera operación id: {operaciones[0].id} (tipo: {type(operaciones[0].id)})")

        # Crear diccionario de facturas agrupadas por operacion_id (STRING)
        # JOIN: operaciones.operation_id = facturas.operacion_id
        facturas_por_operacion = {}
        for factura in todas_facturas:
            op_id = factura.operacion_id
            if op_id not in facturas_por_operacion:
                facturas_por_operacion[op_id] = []
            facturas_por_operacion[op_id].append(factura)

        print(f"[DEBUG] Keys en facturas_por_operacion: {list(facturas_por_operacion.keys())[:5]}")

        result = {
            "operaciones": []
        }

        for op in operaciones:
            # Obtener facturas usando operation_id (STRING)
            facturas_op = facturas_por_operacion.get(op.operation_id, [])
            if not facturas_op:
                print(f"[DEBUG] No facturas para op.operation_id={op.operation_id}")

            operacion_data = {
                "id": op.operation_id,
                "cliente": op.emisor_razon_social or "Sin nombre",
                "monto": float(op.total_monto) if op.total_monto else 0,
                "documentos": len(facturas_op),
                "estado": "cedida ok",  # CONSTANTE (no existe en BD)
                "rucCliente": (op.emisor_rut or "").split('-')[0].replace('.', '').replace(',', '').strip(),
                "ejecutivo": str(op.usuario_id) if op.usuario_id else "Sin asignar",
                "deudores": []
            }

            # Agrupar facturas por deudor
            deudores_dict = {}
            for factura in facturas_op:
                rut = factura.receptor_rut or ""
                rut_normalizado = rut.split('-')[0].replace('.', '').replace(',', '').strip() if rut else ""

                if rut not in deudores_dict:
                    # Gestiones como CONSTANTE VACIA (tabla no existe)
                    deudores_dict[rut] = {
                        "nombre": factura.receptor_razon_social or "Sin nombre",
                        "ruc": rut_normalizado,
                        "facturas": [],
                        "gestiones": [],  # CONSTANTE VACIA
                        "contactos": []  # CONSTANTE VACIA
                    }

                # Agregar factura al deudor
                deudores_dict[rut]["facturas"].append({
                    "folio": str(factura.folio),
                    "tipoDTE": factura.tipo_dte or "33",
                    "montoFactura": float(factura.monto_total) if factura.monto_total else 0,
                    # CONSTANTES (no existen en BD)
                    "aceptado": False,
                    "anulado": False,
                    "reclamado": False,
                    "isVerified": False,
                    "estado": "Pendiente",
                    "fechaEmision": factura.fecha_emision or "",
                    "historialSII": [
                        {
                            "descEvento": "DTE Cedido",
                            "fechaEvento": ""
                        }
                    ],
                    "estadoSII": "Pendiente",
                    "contactos": [],
                    "gestiones": []
                })

            operacion_data["deudores"] = list(deudores_dict.values())
            result["operaciones"].append(operacion_data)

        return result

    except Exception as e:
        print(f"[ERROR] Error obteniendo operaciones: {str(e)}")
        import traceback
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": f"Error obteniendo operaciones: {str(e)}"}
        )


if __name__ == "__main__":
    print("[STARTUP] Iniciando Backend Local en puerto 8080...")
    print("[CONFIG] CORS habilitado para todos los origenes")
    print("[CONFIG] Sin autenticacion (acceso libre)")
    uvicorn.run(app, host="0.0.0.0", port=8080)
