"""Coordina NiFi y Spark. Nunca permite procesamiento de un lote no sellado."""
import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

from start_service import database_url


def run_script(path, arguments):
    print(f"Etapa: {path.name}", flush=True)
    # No se imprime la lista de argumentos porque puede contener credenciales.
    completed = subprocess.run([sys.executable, str(path), *arguments], check=False)
    if completed.returncode:
        # CalledProcessError mostraría la lista completa, incluida una URL con
        # contraseña; conservar únicamente nombre de etapa y código de salida.
        raise RuntimeError(f"La etapa {path.name} terminó con código {completed.returncode}; revisa sus registros")


def prepare_flow(project, api):
    sys.path.insert(0, str(project / "02_Codigo" / "ingesta"))
    from nifi_client import NiFiClient
    state = project / "03_Visualizaciones" / "evidencias" / "nifi_ids.json"
    if state.exists():
        ids = json.loads(state.read_text(encoding="utf-8"))
        try:
            NiFiClient().call("GET", "/process-groups/" + ids["group_id"])
        except urllib.error.HTTPError as exc:
            if exc.code != 404:
                raise
            # Conserva el estado anterior sin reutilizar IDs de un servidor nuevo.
            state.rename(state.with_name(f"nifi_ids_stale_{int(time.time())}.json"))
    run_script(project / "02_Codigo" / "ingesta" / "construir_flujo.py", ["--project-root", str(project), "--api-url", api])


def main():
    import fcntl  # El contenedor es Linux; no se importa durante validación Windows.
    parser = argparse.ArgumentParser()
    parser.add_argument("--batch-id", choices=["base", "delta"], default="base")
    args = parser.parse_args()
    os.umask(0o002)
    project = Path(os.getenv("PROJECT_ROOT", "/project")).resolve()
    data = Path(os.getenv("DATA_ROOT", str(project / "data"))).resolve()
    api = os.getenv("API_URL", "http://api:18080").rstrip("/")
    os.environ["DATABASE_URL"] = database_url()
    if len(os.environ.get("NIFI_PASSWORD", "")) < 12:
        raise ValueError("NIFI_PASSWORD debe tener al menos 12 caracteres")
    lock_path = data / "locks" / "pipeline.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            raise RuntimeError("Hay otro lote en ejecución; espera a que termine") from exc
        if args.batch_id == "delta":
            with __import__("psycopg").connect(os.environ["DATABASE_URL"]) as conn:
                completed_base = conn.execute("SELECT EXISTS (SELECT 1 FROM ecommerce.pipeline_runs WHERE batch_id='base' AND status='success')").fetchone()[0]
            if not completed_base:
                raise RuntimeError("Primero publica correctamente el lote base")
        with urllib.request.urlopen(api + "/health", timeout=10) as response:
            if not json.load(response).get("source_available"):
                raise RuntimeError("La API todavía no tiene sus fuentes generadas")
        prepare_flow(project, api)
        run_script(project / "02_Codigo" / "ingesta" / "ejecutar_lote.py", ["--project-root", str(project), "--batch-id", args.batch_id, "--api-url", api, "--db-url", os.environ["DATABASE_URL"]])
        manifest = data / "manifests" / f"{args.batch_id}.json"
        state = json.loads(manifest.read_text(encoding="utf-8-sig"))
        if state.get("status") not in ("closed", "complete", "sealed") and not state.get("closed"):
            raise RuntimeError("NiFi no declaró el lote cerrado; Spark no se ejecutará")
        run_script(project / "02_Codigo" / "procesamiento" / "job.py", ["--project-root", str(project), "--batch-id", args.batch_id,
            "--jdbc-jar", "/opt/jdbc/postgresql-42.7.7.jar", "--events-dir", str(data / "landing" / "events" / args.batch_id),
            "--catalog-dir", str(data / "landing" / "catalog"), "--master", "local[2]"])
        evidence = data / "evidence" / f"docker_{args.batch_id}_completed.json"
        evidence.parent.mkdir(parents=True, exist_ok=True)
        evidence.write_text(json.dumps({"batch_id": args.batch_id, "completed_at_utc": datetime.now(timezone.utc).isoformat(),
            "status": "completed", "manifest_sha256": hashlib.sha256(manifest.read_bytes()).hexdigest(),
            "deployment": "docker_compose", "stages": ["nifi_closed_manifest", "spark_jdbc_publication"]}, indent=2), encoding="utf-8")
        print(f"Lote {args.batch_id} publicado. Evidencia: {evidence}", flush=True)


if __name__ == "__main__":
    main()
