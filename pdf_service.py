"""
Microservicio de Procesamiento de PDFs
Puerto: 8001
Responsabilidad: Extraer datos de facturas chilenas desde PDF
"""

import fitz  # PyMuPDF
import zxingcpp
from lxml import etree
from PIL import Image
import numpy as np
from io import BytesIO
from typing import Optional, Dict, Any, List
import os
import tempfile
import uvicorn
import PyPDF2
from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import re
from datetime import datetime

app = FastAPI(title="PDF Processing Service", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def _render_fullpage(page: fitz.Page, dpi: int = 300) -> Image.Image:
    """Convierte una página completa de PDF a una imagen."""
    mat = fitz.Matrix(dpi / 72, dpi / 72)
    pix = page.get_pixmap(matrix=mat, alpha=False)
    return Image.open(BytesIO(pix.tobytes("png")))

def _decode_pdf417(img: Image.Image) -> List[str]:
    """Decodifica el código de barras PDF417 de una imagen."""
    payloads = []
    img_gray = img.convert("L")
    arr_gray = np.array(img_gray)

    results = zxingcpp.read_barcodes(
        arr_gray,
        formats=zxingcpp.BarcodeFormat.PDF417,
        try_rotate=True
    )

    for r in results:
        if r.valid and r.text:
            payloads.append(r.text)

    return list(set(payloads))

def _text(el, tag):
    """Función auxiliar para extraer texto de un elemento XML de forma segura."""
    node = el.find(tag)
    return node.text.strip() if node is not None and node.text else None

def parse_ted_payload(payload_xml: str) -> Optional[Dict[str, Any]]:
    """Convierte el XML del Timbre Electrónico en un diccionario de Python."""
    try:
        root = etree.fromstring(payload_xml.encode("utf-8"))
    except Exception:
        return None

    dd_node = root.find(".//DD")
    if dd_node is None:
        return None

    td = _text(dd_node, "TD")
    folio = _text(dd_node, "F")
    fecha_emision = _text(dd_node, "FE")
    monto_total = _text(dd_node, "MNT")
    rut_emisor = _text(dd_node, "RE")
    rut_receptor = _text(dd_node, "RR")
    razon_social_receptor = _text(dd_node, "RSR")
    caf_node = dd_node.find("CAF/DA")
    razon_social_emisor = _text(caf_node, "RS") if caf_node is not None else "No encontrada"

    info_factura = {
        "documento": {
            "tipo_dte": td,
            "folio": int(folio) if folio and folio.isdigit() else folio,
            "fecha_emision": fecha_emision,
            "monto_total": int(monto_total) if monto_total and monto_total.isdigit() else monto_total,
        },
        "emisor": {
            "rut": rut_emisor,
            "razon_social": razon_social_emisor,
        },
        "receptor": {
            "rut": rut_receptor,
            "razon_social": razon_social_receptor,
        }
    }
    return info_factura

def obtener_info_factura_pdf(pdf_path: str) -> Optional[Dict[str, Any]]:
    """Función principal: Abre un PDF, busca, lee y parsea el timbre electrónico."""
    try:
        with fitz.open(pdf_path) as doc:
            for page in doc:
                img = _render_fullpage(page)
                payloads = _decode_pdf417(img)

                if payloads:
                    ted_xml = payloads[0]
                    info_normalizada = parse_ted_payload(ted_xml)
                    if info_normalizada:
                        return info_normalizada
    except Exception as e:
        print(f"Error al abrir o procesar el PDF con PyMuPDF: {e}")
        return None

    return None

# --- Funciones de extracción de texto ---

def _clean_amount(amount_str: Optional[str]) -> Optional[int]:
    """Limpia un string de monto y lo convierte a entero."""
    if not amount_str:
        return None
    cleaned_str = re.sub(r'[\$\.]', '', amount_str).strip()
    return int(cleaned_str)

def _normalize_date(date_str: Optional[str]) -> Optional[str]:
    """Convierte una fecha en formato 'DD de Mes de AAAA' o 'DD-MM-AAAA' a 'AAAA-MM-DD'."""
    if not date_str:
        return None

    date_str = date_str.strip()

    months = {
        "enero": "01", "febrero": "02", "marzo": "03", "abril": "04",
        "mayo": "05", "junio": "06", "julio": "07", "agosto": "08",
        "septiembre": "09", "octubre": "10", "noviembre": "11", "diciembre": "12"
    }
    match_long = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', date_str.lower())
    if match_long:
        day, month_name, year = match_long.groups()
        month_number = months.get(month_name)
        if month_number:
            return f"{year}-{month_number}-{int(day):02d}"

    try:
        dt = datetime.strptime(date_str, '%d-%m-%Y')
        return dt.strftime('%Y-%m-%d')
    except ValueError:
        pass

    return date_str

def _get_first_match(pattern: str, text: str, flags: int = 0) -> Optional[str]:
    """Busca un patrón y devuelve el primer grupo de captura, o None."""
    match = re.search(pattern, text, flags)
    return match.group(1).strip() if match else None

def parse_text_payload(text: str) -> Optional[Dict[str, Any]]:
    """Parsea el texto extraído de una factura para obtener un diccionario estructurado."""
    try:
        ruts = re.findall(r'R\.?U\.?T\.?[:\s\n]+([\d\.]{8,12}-[\dkK])', text)
        rut_emisor = ruts[0] if len(ruts) > 0 else _get_first_match(r'RUT\.:\s*([\d\.]{8,12}-[\dkK])', text)
        rut_receptor = ruts[1] if len(ruts) > 1 else _get_first_match(r'Señor\(es\).*?RUT\s*([\d\.]{8,12}-[\dkK])', text, re.DOTALL)

        razon_social_receptor = _get_first_match(r'Señor\s?\(es\)\s*\n(.*?)\n', text) or \
                                _get_first_match(r'Señor\(es\)(.*?)\s*RUT', text.replace('\n', ' '))

        razon_social_emisor = None
        first_line = text.split('\n')[0].strip()
        if not re.match(r'R\.?U\.?T', first_line, re.IGNORECASE):
            razon_social_emisor = first_line

        if not razon_social_emisor:
            emisor_block_match = re.search(r'Fecha\s+de\s+Vencimiento.*?\n(.*?)\n', text, re.DOTALL)
            if emisor_block_match:
                candidate = emisor_block_match.group(1).strip()
                if not re.match(r'\d{2}-\d{2}-\d{4}', candidate):
                     razon_social_emisor = candidate

        tipo_dte = "34" if "FACTURA EXENTA" in text.upper() else "33" if "FACTURA ELECTRÓNICA" in text.upper() else None
        folio = _get_first_match(r'(?:Folio N°|Nº)\s*(\d+)', text) or _get_first_match(r'(?:FACTURA ELECTRÓNICA)\s*Nº\s*(\d+)', text, re.IGNORECASE)

        fecha_emision_raw = _get_first_match(r'Fecha Emisión\s*(\d{1,2} de \w+ de \d{4})', text) or \
                            _get_first_match(r'Fecha Documento\s*(\d{1,2}-\d{1,2}-\d{4})', text)

        monto_total_matches = re.findall(r'(?:Monto Total|Total)\s*\$?\s*([\d\.,]+)', text)
        monto_total = monto_total_matches[-1] if monto_total_matches else None

        glosa = _get_first_match(r'DETALLES\n.*?\n\d\s*(.*?)\n', text, re.DOTALL)
        if not glosa:
             glosa_match = re.search(r'\d\s+[A-Z0-9]+\s+(.*?)\s+\d+\s+UN', text)
             if glosa_match:
                 glosa = glosa_match.group(1).strip()

        if not all([folio, monto_total]):
            return None

        return {
            "documento": {
                "tipo_dte": tipo_dte,
                "folio": int(folio) if folio else None,
                "fecha_emision": _normalize_date(fecha_emision_raw),
                "monto_total": _clean_amount(monto_total),
            },
            "emisor": {
                "rut": rut_emisor,
                "razon_social": razon_social_emisor,
            },
            "receptor": {
                "rut": rut_receptor,
                "razon_social": razon_social_receptor,
            },
            "glosa": glosa
        }
    except Exception as e:
        print(f"Error al parsear el contenido del texto: {e}")
        return None

# --- Endpoints ---

@app.post("/procesar-pdf/")
async def procesar_pdf(file: UploadFile = File(...)):
    """
    Procesa un PDF de factura chilena.
    Intenta extraer datos del código de barras PDF417 (TED).
    Si falla, extrae y parsea el texto del documento.
    """
    if not file.filename.lower().endswith('.pdf'):
        raise HTTPException(status_code=400, detail="El archivo debe ser un PDF")

    with tempfile.NamedTemporaryFile(delete=False, suffix='.pdf') as tmp_file:
        content = await file.read()
        tmp_file.write(content)
        tmp_path = tmp_file.name

    try:
        # 1. Intentar leer desde el código de barras
        informacion = obtener_info_factura_pdf(tmp_path)

        if informacion:
            return JSONResponse(content={
                "success": True,
                "data": informacion,
                "source": "barcode"
            })

        # 2. Si falla, intentar leer y parsear el texto
        else:
            text_content = ""
            try:
                with open(tmp_path, 'rb') as pdf_file_obj:
                    pdf_reader = PyPDF2.PdfReader(pdf_file_obj)
                    for page in pdf_reader.pages:
                        text_content += page.extract_text() or ""
            except Exception as e:
                print(f"No se pudo leer el PDF con PyPDF2: {e}")
                raise HTTPException(status_code=500, detail="No se pudo procesar el PDF para extracción de texto.")

            if text_content:
                parsed_data = parse_text_payload(text_content)
                if parsed_data:
                    return JSONResponse(content={
                        "success": True,
                        "data": parsed_data,
                        "source": "text_extraction"
                    })

            raise HTTPException(status_code=404, detail="No se pudo encontrar un código de barras válido ni extraer datos estructurados del texto del PDF.")

    except HTTPException as e:
        raise e
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Ocurrió un error inesperado: {str(e)}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

@app.get("/health")
async def health():
    return {"status": "healthy", "service": "pdf_service"}

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8001))
    print(f"Iniciando PDF Service en puerto {port}...")
    uvicorn.run(app, host="0.0.0.0", port=port)
