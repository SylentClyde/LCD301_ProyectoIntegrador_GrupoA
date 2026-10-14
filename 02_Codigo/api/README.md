# API REST — Grupo A

Ejecutar después de generar las fuentes. Desde `02_Codigo`:

```powershell
python -m uvicorn api.main:app --host 127.0.0.1 --port 18080
```

Variables opcionales: `PROJECT_ROOT`, `SOURCE_DATA_DIR`, `API_RATE_PER_MINUTE` (120 consultas por minuto por defecto), `DATABASE_URL` para consultar PostgreSQL. Los eventos no requieren PostgreSQL.

| Ruta | Resultado |
| --- | --- |
| `/health` | Estado del servicio y disponibilidad de fuentes. |
| `/events?cursor=0&limit=50000` | Lote inicial completo para InvokeHTTP de NiFi. |
| `/events?cursor=0&limit=1000` | Página inicial; continuar con `next_cursor` hasta que sea `null`. |
| `/events/delta?cursor=0&limit=50000` | Lote independiente de siete eventos para demostrar actualización. |
| `/events?dataset=delta` | Alternativa equivalente para el lote adicional. |
| `/schema/event.v1` | Contrato JSON Schema usado por NiFi. |
| `/kpis?date_from=2026-04-01&date_to=2026-09-30&channel=web` | KPIs persistidos en `ecommerce.fact_event`, con filtros opcionales. |
| `/metrics` | Contadores de solicitudes, eventos retornados, errores y latencia acumulada de la API. |
| `/docs` | Documentación interactiva de FastAPI. |

Contrato de página: `{batch_id,source,cursor,next_cursor,events}`; incluye también `dataset` y `total_records`. `batch_id` depende del contenido del archivo y rango de página. Repetir una URL devuelve la misma página y el mismo ID sin acuse destructivo: la idempotencia se resuelve en NiFi/Spark/PostgreSQL.

El límite global devuelve HTTP 429 y `Retry-After`; NiFi debe reenviar después de esa espera. Un archivo ausente devuelve 503 sin exponer rutas privadas. `/kpis` devuelve 503 cuando la conexión o el esquema no están disponibles y no expone credenciales.

Para verificar HTTP y determinismo, ejecutar `python api/verificar_api.py --project-root ..`. Tras publicar todos los datos, añadir `--expect-published after_delta` para reconciliar los KPIs persistidos. `fuentes/verificar_fuentes.py` verifica además el ciclo de vida completo de los pedidos y, cuando jsonschema está instalado, el rechazo de los veinte errores estructurales.

Dependencias externas de este servicio: FastAPI, Uvicorn y psycopg2-binary para `/kpis`. El generador usa solamente Python estándar. Los contadores `/metrics` reinician con el proceso; no equivalen a eventos publicados en PostgreSQL ni a throughput de todo el pipeline. La latencia de este contador mide lectura y preparación de la página en el manejador, sin transmisión HTTP.
