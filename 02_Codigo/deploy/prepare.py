"""Inicializa código y fuentes en volúmenes Docker separados del laboratorio local."""
import os
import shutil
import subprocess
import sys
from pathlib import Path


def main():
    os.umask(0o002)
    source = Path("/source")
    project = Path("/project")
    code = project / "02_Codigo"
    code.mkdir(parents=True, exist_ok=True)
    # Copia solo fuentes de código. No importa IDs de NiFi ni capturas locales.
    for folder in ("api", "fuentes", "ingesta", "procesamiento", "sql", "pruebas", "almacenamiento"):
        shutil.copytree(source / "02_Codigo" / folder, code / folder, dirs_exist_ok=True,
                        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "spark_fixture", "*.log"))
    shutil.copytree(source / "03_Visualizaciones" / "dashboard", project / "03_Visualizaciones" / "dashboard", dirs_exist_ok=True,
                    ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "verification_report.json"))
    data = project / "data"
    folders = ["source", "inbox/catalog", "landing/events/base", "landing/events/delta", "landing/catalog", "raw/api",
               "quality/ingesta/originales", "quality/ingesta/metadatos", "quality/processing", "manifests", "processed", "evidence", "locks"]
    for folder in folders:
        path = data / folder
        path.mkdir(parents=True, exist_ok=True)
    # La imagen oficial NiFi utiliza uid/gid 1000. Setgid conserva permisos
    # compartidos cuando el coordinador crea archivos dentro del volumen.
    for path in [data] + [p for p in data.rglob("*") if p.is_dir()]:
        os.chown(path, 1000, 1000)
        os.chmod(path, 0o2775)
    subprocess.run([sys.executable, str(code / "fuentes" / "generar_datos.py"), "--project-root", str(project)], check=True)
    print("Fuentes reproducibles preparadas. No se copiaron estados de NiFi del host.")


if __name__ == "__main__":
    main()

