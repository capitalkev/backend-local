from pydantic import BaseModel
from typing import List

class ContactosRequest(BaseModel):
    ruc_cliente: str
    emails: List[str]
    ruc_deudor: str