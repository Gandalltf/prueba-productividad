import json
import io
import base64

import azure.functions as func
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A4

from .nomina_logic import process


def main(req: func.HttpRequest) -> func.HttpResponse:
    # Leer cuerpo JSON
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Leer registros (items o records)
    raw_records = body.get("items") or body.get("records") or []

    if isinstance(raw_records, dict):
        # Caso raro: dict -> si tiene "value" usamos esa lista, si no lo metemos en una lista
        if "value" in raw_records and isinstance(raw_records["value"], list):
            records = raw_records["value"]
        else:
            records = [raw_records]
    elif isinstance(raw_records, list):
        records = raw_records
    else:
        records = []

    # Fechas
    start_date = body.get("start_date") or (body.get("range") or {}).get("start")
    end_date = body.get("end_date") or (body.get("range") or {}).get("end")

    # Parámetros opcionales
    tz = body.get("timezone", "Europe/Madrid")
    selected_worker = body.get("worker_filter")
    flexible = body.get("descanso_flexible_periods")
    enforce_sunday_rest = bool(body.get("enforce_sunday_rest", True))

    if not start_date or not end_date:
        return func.HttpResponse("Missing start/end date", status_code=400)

    # Llamar a la lógica de nóminas
    try:
        result = process(
            records,
            start_date,
            end_date,
            tz,
            selected_worker,
            flexible,
            enforce_sunday_rest,
        )
    except Exception as e:
        return func.HttpResponse(f"Processing error: {e}", status_code=500)

    workers = result.get("workers", [])

    # ----------------------------------------------------------------------
    # Generar PDF en memoria (resumen por trabajador)
    # ----------------------------------------------------------------------
    buffer = io.BytesIO()
    c = canvas.Canvas(buffer, pagesize=A4)
    width, height = A4
    y = height - 50

    c.setFont("Helvetica-Bold", 14)
    c.drawString(50, y, "Resumen de nómina")
    y -= 25

    c.setFont("Helvetica", 11)
    c.drawString(50, y, f"Periodo: {start_date} - {end_date}")
    y -= 20

    if not workers:
        c.drawString(50, y, "Sin datos de trabajadores en el periodo seleccionado.")
    else:
        for w in workers:
            if y < 100:
                c.showPage()
                y = height - 50
                c.setFont("Helvetica", 11)

            nombre = w.get("trabajador", "Trabajador")
            mes = w.get("mes", "")
            anio = w.get("anio", "")
            tot = w.get("totales", {})
            fich = tot.get("fichaje", 0)
            prod = tot.get("productividad", 0)

            c.setFont("Helvetica-Bold", 12)
            c.drawString(50, y, f"{nombre} - {mes} {anio}")
            y -= 18

            c.setFont("Helvetica", 11)
            c.drawString(70, y, f"Total fichaje: {fich} horas")
            y -= 15
            c.drawString(70, y, f"Total productividad restante: {prod} horas")
            y -= 25

    c.save()
    pdf_bytes = buffer.getvalue()
    buffer.close()

    # Base64
    pdf_b64 = base64.b64encode(pdf_bytes).decode("utf-8")

    # Nombre de archivo
    if workers:
        nombre_trabajador = workers[0].get("trabajador", "Nomina")
        mes = workers[0].get("mes", "")
        anio = workers[0].get("anio", "")
        filename = f"Nomina_{nombre_trabajador}_{mes}_{anio}.pdf"
    else:
        filename = "Nomina_sin_datos.pdf"

    # Respuesta combinando todo
    response = {
        "workers": workers,        # JSON completo (con html, días, totales, etc.)
        "pdf_base64": pdf_b64,     # PDF en base64
        "filename": filename       # Nombre sugerido
    }

    return func.HttpResponse(
        json.dumps(response, ensure_ascii=False),
        mimetype="application/json"
    )
