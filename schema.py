from pydantic import BaseModel
from typing import List

class ContactosRequest(BaseModel):
    ruc_cliente: str
    emails: List[str]
    ruc_deudor: str


class EditarContactoRequest(BaseModel):
    ruc_cliente: str
    ruc_deudor: str
    email_viejo: str
    email_nuevo: str


class EliminarContactoRequest(BaseModel):
    ruc_cliente: str
    ruc_deudor: str
    email: str