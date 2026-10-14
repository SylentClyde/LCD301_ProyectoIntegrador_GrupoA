"""API REST de lectura; repetir una página no consume ni modifica datos."""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from collections import deque
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Literal

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse, PlainTextResponse

PROJECT_ROOT = Path(os.getenv("PROJECT_ROOT", Path(__file__).resolve().parents[2])).resolve()
SOURCE_DIR = Path(os.getenv("SOURCE_DATA_DIR", PROJECT_ROOT / "data" / "source")).resolve()
RATE_PER_MINUTE = max(1, int(os.getenv("API_RATE_PER_MINUTE", "120")))
app = FastAPI(title="LCD301 Grupo A — fuente e-commerce", version="1.0.0",
              description="Datos sintéticos, moneda BOB y fechas UTC. No hay datos personales reales.")
_cache: dict[str, tuple[int, list[dict], str]] = {}
_lock = threading.Lock()
_requests: deque[float] = deque()
_metrics = {"event_requests": 0, "events_returned": 0, "rate_limit_errors": 0,
            "source_errors": 0, "latency_seconds_sum": 0.0}


def read_dataset(dataset: str) -> tuple[list[dict], str]:
    path = SOURCE_DIR / f"events_{dataset}.json"
    try:
        version = path.stat().st_mtime_ns
        with _lock:
            cached = _cache.get(dataset)
            if cached and cached[0] == version:
                return cached[1], cached[2]
        raw = path.read_bytes()
        rows = json.loads(raw)
        if not isinstance(rows, list):
            raise ValueError("La fuente debe ser una lista JSON")
        digest = hashlib.sha256(raw).hexdigest()[:12]
        with _lock:
            _cache[dataset] = (version, rows, digest)
        return rows, digest
    except (OSError, ValueError) as error:
        with _lock:
            _metrics["source_errors"] += 1
        raise HTTPException(status_code=503, detail="Fuente no disponible; ejecutar fuentes/generar_datos.py") from error


def enforce_rate() -> None:
    now = time.monotonic()
    with _lock:
        while _requests and _requests[0] <= now - 60:
            _requests.popleft()
        if len(_requests) >= RATE_PER_MINUTE:
            _metrics["rate_limit_errors"] += 1
            retry = max(1, int(60 - (now - _requests[0])) + 1)
            raise HTTPException(status_code=429, detail="Límite global de consultas por minuto alcanzado",
                                headers={"Retry-After": str(retry)})
        _requests.append(now)


@app.get("/health")
def health() -> dict:
    available = all((SOURCE_DIR / f"events_{name}.json").exists() for name in ("initial", "delta"))
    return {"status": "ok" if available else "source_not_generated", "service": "lcd301-source-api",
            "source_available": available, "currency": "BOB", "timezone": "UTC",
            "rate_limit_requests_per_minute": RATE_PER_MINUTE}


@app.get("/events")
def events(cursor: int = Query(0, ge=0), limit: int = Query(1000, ge=1, le=50000),
           dataset: Literal["initial", "delta"] = "initial") -> dict:
    started = time.perf_counter()
    enforce_rate()
    rows, digest = read_dataset(dataset)
    page = rows[cursor:cursor + limit]
    next_cursor = cursor + len(page) if cursor + len(page) < len(rows) else None
    with _lock:
        _metrics["event_requests"] += 1
        _metrics["events_returned"] += len(page)
        _metrics["latency_seconds_sum"] += time.perf_counter() - started
    return {"batch_id": f"api-{dataset}-{digest}-{cursor:06}-{len(page):06}",
            "source": "rest_api", "dataset": dataset, "cursor": cursor,
            "next_cursor": next_cursor, "total_records": len(rows), "events": page}


@app.get("/events/delta")
def delta(cursor: int = Query(0, ge=0), limit: int = Query(1000, ge=1, le=50000)) -> dict:
    return events(cursor, limit, "delta")


@app.get("/schema/event.v1")
def schema() -> JSONResponse:
    schema_path = PROJECT_ROOT / "02_Codigo" / "fuentes" / "event.v1.schema.json"
    return JSONResponse(json.loads(schema_path.read_text(encoding="utf-8")))


@app.get("/metrics", response_class=PlainTextResponse)
def metrics() -> str:
    with _lock:
        values = dict(_metrics)
    lines = []
    for metric, value in values.items():
        lines.extend((f"# TYPE lcd301_api_{metric} counter", f"lcd301_api_{metric} {value}"))
    return "\n".join(lines) + "\n"


@app.get("/kpis")
def kpis(date_from: date | None = None, date_to: date | None = None,
         channel: Literal["web", "movil", "tienda"] | None = None) -> dict:
    if date_from and date_to and date_from > date_to:
        raise HTTPException(422, "date_from debe ser anterior o igual a date_to")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise HTTPException(503, "Consulta de negocio no configurada: definir DATABASE_URL")
    try:
        import psycopg2
        import psycopg2.extras
        with closing(psycopg2.connect(database_url, connect_timeout=5, options="-c timezone=UTC")) as connection:
            connection.set_session(readonly=True, autocommit=True)
            with connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT count(*) AS events,
                      count(*) FILTER (WHERE event_type='created') AS created_orders,
                      count(*) FILTER (WHERE event_type='paid') AS paid_orders,
                      count(*) FILTER (WHERE event_type='cancelled') AS cancelled_orders,
                      count(*) FILTER (WHERE event_type='refunded') AS refund_events,
                      count(DISTINCT customer_id) FILTER (WHERE event_type='paid') AS paying_customers,
                      coalesce(sum(total_cents) FILTER (WHERE event_type='paid'),0)::bigint AS gross_cents,
                      coalesce(sum(total_cents) FILTER (WHERE event_type='refunded'),0)::bigint AS refund_cents
                    FROM ecommerce.fact_event
                    WHERE (%s::date IS NULL OR event_time >= %s::date)
                      AND (%s::date IS NULL OR event_time < %s::date + interval '1 day')
                      AND (%s::text IS NULL OR channel=%s)
                """, (date_from, date_from, date_to, date_to, channel, channel))
                result = dict(cursor.fetchone())
                cursor.execute("""SELECT batch_id,finished_at FROM ecommerce.pipeline_runs
                                  WHERE status IN ('success','completed','published')
                                  ORDER BY finished_at DESC NULLS LAST LIMIT 1""")
                latest = cursor.fetchone()
        result["net_cents"] = result["gross_cents"] - result["refund_cents"]
        result.update(currency="BOB", timezone="UTC", filters={"date_from": date_from, "date_to": date_to,
                       "channel": channel}, latest_batch=dict(latest) if latest else None)
        return result
    except Exception as error:
        # No devolver la URL de conexión ni credenciales en el error HTTP.
        raise HTTPException(503, "PostgreSQL no disponible o esquema todavía no publicado") from error


@app.exception_handler(404)
async def not_found(request: Request, exception: HTTPException) -> JSONResponse:
    return JSONResponse({"detail": "Ruta inexistente; documentación disponible en /docs"}, status_code=404)
