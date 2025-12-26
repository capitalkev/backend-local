import uvicorn
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy.orm import Session
from schema import ContactosRequest
from database import get_db
from crud import (
    listar_operaciones,
    obtener_contactos,
    obtener_email_filtrados,
    agregar_contactos,
    extraer_drive,
    obtener_operacion_completa,
)

from gmail_service import enviar_correo_multiples, autenticar_gmail


app = FastAPI(
    title="Backend Local - Operaciones",
    version="1.0.0",
    description="Backend simple sin autenticación para frontend-local",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Endpoint raíz"""
    return {
        "message": "Backend Local - Operaciones",
        "version": "1.0.0",
        "description": "Backend simple sin autenticación para desarrollo local",
        "endpoints": {"operaciones": "GET /operaciones/ - Lista todas las operaciones"},
    }


@app.get("/health")
async def health():
    """Health check"""
    return {"status": "healthy", "service": "backend-local"}


@app.get("/operaciones/")
async def listar_operaciones_endpoint(db: Session = Depends(get_db)):
    try:
        # 1. Llamamos a la función ligera del CRUD
        filas = listar_operaciones(db)

        resultado = []
        for op in filas:
            # Construimos el objeto simple para la tabla del frontend
            resultado.append(
                {
                    "id": op["operation_id"],
                    "cliente": op["emisor_razon_social"] or "Sin nombre",
                    "monto": float(op["total_monto"]) if op["total_monto"] else 0,
                    "rucCliente": (op["emisor_rut"] or "").split("-")[0].strip(),
                    "ejecutivo": str(op["usuario_id"])
                    if op["usuario_id"]
                    else "Sin asignar",
                    "estado": "cedida ok",
                }
            )

        return {"operaciones": resultado}

    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/operaciones/{op_id}")
async def obtener_detalle_operacion_endpoint(op_id: str, db: Session = Depends(get_db)):
    """
    Devuelve la estructura compleja (Deudores -> Facturas) para UNA operación.
    """
    try:
        resultado = obtener_operacion_completa(db, op_id)

        if not resultado:
            return JSONResponse(
                status_code=404,
                content={"message": "Operación no encontrada o sin facturas"},
            )

        return resultado

    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/contactos/{rut_receptor}")
async def obtener_contactos_endpoint(rut_receptor: str, db: Session = Depends(get_db)):
    """
    Devuelve los contactos asociados a un receptor específico.
    """
    try:
        contactos = obtener_contactos(db, rut_receptor)

        if not contactos:
            return JSONResponse(
                status_code=404,
                content={
                    "message": "No se encontraron contactos para el receptor especificado"
                },
            )

        return {"contactos": contactos}

    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.get("/email-filtrados/")
async def obtener_email_filtrados_endpoint(
    ruc_cliente: str,
    ruc_deudor: str,
    db: Session = Depends(get_db),
):
    """
    Devuelve los emails filtrados para un cliente y deudor específicos.
    """
    try:
        emails = obtener_email_filtrados(db, ruc_cliente, ruc_deudor)

        if not emails:
            return JSONResponse(
                status_code=404,
                content={
                    "message": "No se encontraron emails filtrados para el cliente y deudor especificados"
                },
            )

        return {"emails": emails}

    except Exception as e:
        print(f"[ERROR] {e}")
        return JSONResponse(status_code=500, content={"error": str(e)})


@app.post("/add-contactos/")
async def agregar_contactos_endpoint(
    solicitud: ContactosRequest,
    db: Session = Depends(get_db),
):
    """
    Agrega emails a un deudor (ruc_deudor).
    Body esperado:
    {
      "ruc_deudor": "12345678",
      "emails": ["correo1@empresa.com", "correo2@empresa.com"]
    }
    """
    try:
        agregar_contactos(
            db=db,
            ruc_cliente=solicitud.ruc_cliente,
            emails=solicitud.emails,
            ruc_deudor=solicitud.ruc_deudor,
        )

        return {
            "status": "success",
            "message": f"Se procesaron {len(solicitud.emails)} contactos para el RUT {solicitud.ruc_deudor}",
        }

    except Exception as e:
        print(f"[ERROR] Al agregar contactos: {str(e)}")
        return JSONResponse(
            status_code=500, content={"error": f"Error interno: {str(e)}"}
        )


@app.post("/send-gmail/{destinatarios}/{op_id}")
async def send_gmail(destinatarios: str, op_id: str, db: Session = Depends(get_db)):
    """
    Envía un correo electrónico a múltiples destinatarios usando Gmail API.
    """
    servicio = autenticar_gmail()
    if servicio:
        drive_url = extraer_drive(db, op_id)
        resultado = enviar_correo_multiples(
            servicio, destinatarios, op_id, db, drive_url
        )
        if resultado:
            return {
                "status": "success",
                "message": "Correo enviado correctamente",
                "details": resultado,
            }
        else:
            return JSONResponse(
                status_code=500,
                content={"error": "No se pudo enviar el correo"},
            )
    else:
        return JSONResponse(
            status_code=500,
            content={"error": "No se pudo crear el servicio de Gmail"},
        )


if __name__ == "__main__":
    uvicorn.run(app, host="localhost", port=8080)
