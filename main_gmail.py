from fastapi import FastAPI, HTTPException
from typing import Dict

from config import HOST, PORT
from gmail_service import (
    autenticar_gmail,
    enviar_correo_multiples,
    reenviar_a_multiples_destinatarios,
)

app = FastAPI(
    title="Gmail Service API",
    description="Servicio REST para envío y reenvío de correos electrónicos vía Gmail API",
    version="2.0.0",
)


@app.get("/")
def root():
    """Endpoint raíz con información de la API"""
    return {
        "service": "Gmail Service API",
        "version": "2.0.0",
        "endpoints": {
            "enviar": "/enviar-correo-multiples?destinatarios=email1,email2",
            "reenviar": "/reenviar-multiples/{mensaje_id}?destinatarios=email1,email2",
        },
    }


@app.get("/enviar-correo-multiples")
def enviar_correo_multiples_endpoint(destinatarios: str) -> Dict:
    """
    Envía un correo electrónico a uno o múltiples destinatarios

    Args:
        destinatarios: Emails separados por comas (query parameter)

    Returns:
        Dict con mensaje_id, lista de destinatarios y thread_id

    Ejemplo:
        GET /enviar-correo-multiples?destinatarios=email1@test.com,email2@test.com
    """
    creds = autenticar_gmail()
    resultado = enviar_correo_multiples(creds, destinatarios)

    if resultado is None:
        raise HTTPException(status_code=500, detail="Error al enviar el correo")

    return resultado


@app.get("/reenviar-multiples/{mensaje_id}")
def reenviar_multiples_endpoint(mensaje_id: str, destinatarios: str) -> Dict:
    """
    Responde a un correo enviándolo a múltiples destinatarios (mantiene el hilo)

    Args:
        mensaje_id: ID del mensaje original
        destinatarios: Emails separados por comas (query parameter)

    Returns:
        Dict con mensaje_id, lista de destinatarios y thread_id

    Ejemplo:
        GET /reenviar-multiples/abc123?destinatarios=email1@test.com,email2@test.com
    """
    creds = autenticar_gmail()
    resultado = reenviar_a_multiples_destinatarios(creds, mensaje_id, destinatarios)

    if resultado is None:
        raise HTTPException(
            status_code=500,
            detail="Error al enviar respuesta a múltiples destinatarios",
        )

    return resultado


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host=HOST, port=PORT)
