import json
import azure.functions as func
from .nomina_logic import process


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        body = req.get_json()
    except ValueError:
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Aceptamos tanto "items" como "records"
    records = body.get("items") or body.get("records") or []

    # Fechas (usamos directamente start_date / end_date como tú envías)
    start_date = body.get("start_date") or (body.get("range") or {}).get("start")
    end_date = body.get("end_date") or (body.get("range") or {}).get("end")

    # Parámetros opcionales con valores por defecto
    tz = body.get("timezone", "Europe/Madrid")
    selected_worker = body.get("worker_filter")
    flexible = body.get("descanso_flexible_periods")  # puede ser None, lo maneja nomina_logic
    enforce_sunday_rest = bool(body.get("enforce_sunday_rest", True))

    if not start_date or not end_date:
        return func.HttpResponse("Missing start/end date", status_code=400)

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
        # Muy útil: verás el texto del error en la respuesta si algo peta
        return func.HttpResponse(f"Processing error: {e}", status_code=500)

    return func.HttpResponse(
        json.dumps(result, ensure_ascii=False),
        mimetype="application/json"
    )
