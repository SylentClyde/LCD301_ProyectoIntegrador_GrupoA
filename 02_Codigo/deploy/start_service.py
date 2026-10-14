"""Arranque sin comandos de shell; construye una URL con contraseñas codificadas."""
import os
import sys
from urllib.parse import quote


def database_url():
    if os.getenv("DATABASE_URL"):
        return os.environ["DATABASE_URL"]
    password = os.environ["PGPASSWORD"]
    user = quote(os.getenv("PGUSER", "lcd301"), safe="")
    database = quote(os.getenv("PGDATABASE", "lcd301"), safe="")
    return f"postgresql://{user}:{quote(password, safe='')}@{os.getenv('PGHOST', 'postgres')}:{os.getenv('PGPORT', '5432')}/{database}"


def main():
    os.environ["DATABASE_URL"] = database_url()
    mode = sys.argv[1]
    if mode == "api":
        os.chdir("/project/02_Codigo")
        args = [sys.executable, "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "18080"]
    elif mode == "dashboard":
        os.chdir("/project/03_Visualizaciones/dashboard")
        args = [sys.executable, "app.py"]
    else:
        raise ValueError("Modo esperado: api o dashboard")
    os.execv(sys.executable, args)


if __name__ == "__main__":
    main()

