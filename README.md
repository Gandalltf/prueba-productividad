# pruebaSalsa — Azure Functions (Python)

Función HTTP de prueba.

## Probar local (opcional)
- Requisitos: Python 3.10, Azure Functions Core Tools (`func`).
- Instala deps: `pip install -r requirements.txt`
- Ejecuta: `func start` (si tienes el host local configurado).

## Desplegar con GitHub Actions
- Rama: `development`
- Workflow: `.github/workflows/deploy-func.yml`
- Requiere secretos ya configurados (OIDC) y un Function App existente (Python 3.10).

## Probar en Azure
Una vez desplegado, ve a **Funciones** → `Creacion_Nominas` → **Probar** o llama por HTTP:

```bash
curl -X POST "https://<TU_APP>.azurewebsites.net/api/Creacion_Nominas?code=<FUNCTION_KEY>" ^
  -H "Content-Type: application/json" ^
  -d "{\"name\":\"Carlos\"}"
