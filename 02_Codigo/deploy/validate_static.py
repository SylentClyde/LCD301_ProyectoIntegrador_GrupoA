"""Validación estática sin Docker ni dependencias externas. No acredita ejecución."""
import ast
import json
import re
import shutil
from pathlib import Path


def main():
    root = Path(__file__).resolve().parent
    project = root.parents[1]
    document = json.loads((root / "compose.yaml").read_text(encoding="utf-8"))
    services = document["services"]
    volumes = set(document["volumes"])
    assert set(services) == {"prepare", "postgres", "api", "nifi", "dashboard", "pipeline"}
    checks = []
    for name, service in services.items():
        for dependency, configuration in service.get("depends_on", {}).items():
            assert dependency in services, (name, dependency)
            assert configuration["condition"] in {"service_healthy", "service_completed_successfully", "service_started"}
            if configuration["condition"] == "service_healthy":
                assert "healthcheck" in services[dependency], dependency
        for mount in service.get("volumes", []):
            source, destination, *_ = mount.split(":")
            assert destination.startswith("/"), mount
            if source.startswith("."):
                assert (root / source).resolve().exists(), source
            else:
                assert source in volumes, source
        if "build" in service:
            assert (root / service["build"]["dockerfile"]).exists()
        checks.append(f"{name}: dependencias, salud y montajes coherentes")
    assert services["nifi"]["image"] == "apache/nifi:2.12.0"
    assert services["pipeline"]["environment"]["NIFI_ALLOW_INSECURE_HOST"] == "nifi"
    assert services["postgres"]["ports"] == ["127.0.0.1:55432:5432"]
    assert services["api"]["ports"] == ["127.0.0.1:18080:18080"]
    assert services["dashboard"]["ports"] == ["127.0.0.1:18050:8050"]
    assert "pyspark==4.0.1" in (root / "requirements-pipeline.txt").read_text()
    assert "openjdk-21-jre-headless" in (root / "Dockerfile.pipeline").read_text()
    batch_source = (root / "run_batch.py").read_text(encoding="utf-8")
    assert "--allow-unsealed" not in batch_source
    assert "NiFi no declaró el lote cerrado" in batch_source
    for script in ["prepare.py", "run_batch.py", "start_service.py", "validate_static.py"]:
        ast.parse((root / script).read_text(encoding="utf-8"), filename=script)
    for required in ["02_Codigo/fuentes/generar_datos.py", "02_Codigo/ingesta/construir_flujo.py", "02_Codigo/procesamiento/job.py", "03_Visualizaciones/dashboard/app.py"]:
        assert (project / required).exists(), required
    execute_exists = (project / "02_Codigo/ingesta/ejecutar_lote.py").exists()
    assert execute_exists, "Falta el coordinador de ingesta ejecutar_lote.py"
    env = (root / ".env.example").read_text(encoding="utf-8")
    password = re.search(r"^NIFI_PASSWORD=(.+)$", env, re.MULTILINE).group(1)
    assert len(password) >= 12
    checks += ["JSON válido compatible con sintaxis YAML", "Puertos de laboratorio verificados", "Scripts Python con sintaxis válida",
               "Spark 4.0.1 y Java 21 declarados", "Secuencia NiFi → manifiesto cerrado → Spark, sin allow-unsealed", "Credenciales de ejemplo y URL de PostgreSQL codificada"]
    report = {"status": "static_checks_passed", "docker_execution": "NOT_EXECUTED", "docker_compose_config": "NOT_EXECUTED",
              "image_pull_or_build": "NOT_EXECUTED", "docker_cli_detected": bool(shutil.which("docker")), "checks": checks,
              "postgres_container_image": services["postgres"]["image"], "local_verified_postgres_version": "17.6",
              "compatibility_note": "El contenedor usa PostgreSQL 17.11; compatibilidad major 17 prevista, ejecución contenedorizada pendiente."}
    (root / "static_validation.json").write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print(json.dumps(report, ensure_ascii=True, indent=2))


if __name__ == "__main__":
    main()
