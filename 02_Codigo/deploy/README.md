# Despliegue Docker Compose · Grupo A

**Estado: NO EJECUTADO en Docker.** Este equipo de preparación no tiene Docker disponible. `static_validation.json` registra únicamente validación de estructura, rutas, versiones declaradas y sintaxis. No demuestra descarga de imágenes, compilación, arranque de contenedores ni ejecución del pipeline contenedorizado. Las evidencias de la ejecución local se conservan por separado.

El despliegue contiene NiFi 2.12.0, Spark 4.0.1 con Java 21, PostgreSQL 17, la API REST y ambos dashboards. La imagen PostgreSQL especificada es `17.11-bookworm`, una etiqueta publicada en la [documentación oficial de la imagen](https://hub.docker.com/_/postgres). El laboratorio local se verificó con PostgreSQL 17.6; la compatibilidad entre estas versiones del mismo major queda por probar en Docker.

## Requisitos e inicio

Docker Engine o Docker Desktop con contenedores Linux y Docker Compose v2. Planificar 8 GB de memoria disponibles para este laboratorio; este presupuesto no fue medido en Docker. La construcción requiere acceso a las imágenes, paquetes Python, paquetes Debian y JDBC. Los puertos 8443, 18080, 18050 y 55432 deben estar libres; detener primero los servicios del laboratorio local si ocupan esos puertos.

Desde `02_Codigo/deploy`:

```powershell
Copy-Item .env.example .env
docker compose build
docker compose up -d postgres api nifi dashboard
docker compose ps
docker compose run --rm pipeline --batch-id base
docker compose run --rm pipeline --batch-id delta
```

En Bash, el primer paso equivalente es `cp .env.example .env`. Las claves de `.env.example` son exclusivamente de ejemplo para el laboratorio. NiFi exige al menos 12 caracteres. `.env` se excluye del contexto de construcción y del control de versiones. Las contraseñas PostgreSQL se codifican al construir la URL para admitir caracteres especiales sin romper la conexión.

| Servicio | Dirección desde el host | Dirección dentro de Compose |
| --- | --- | --- |
| API REST | http://127.0.0.1:18080/docs | http://api:18080 |
| Dashboard | http://127.0.0.1:18050 | http://dashboard:8050 |
| NiFi | https://localhost:8443/nifi | https://nifi:8443/nifi-api |
| PostgreSQL | 127.0.0.1:55432, base lcd301 | postgres:5432 |

Abrir NiFi con las credenciales de `.env`; el certificado es generado por la instancia del laboratorio. La verificación TLS se desactiva únicamente para el host interno `nifi` autorizado con `NIFI_ALLOW_INSECURE_HOST`. El cliente conserva el comportamiento limitado a localhost fuera de este despliegue. Estas opciones son para el laboratorio y no acreditan configuración de certificados de producción.

El `depends_on` utiliza salud del servicio y finalización exitosa del inicializador, siguiendo la [documentación de orden de arranque de Compose](https://docs.docker.com/compose/how-tos/startup-order/). Spark 4.0.1 admite Java 21 y Python 3.9 o posterior según [Apache Spark](https://spark.apache.org/docs/4.0.1/); aquí se instala PySpark 4.0.1 en Python 3.12 y Java 21. Es ejecución `local[2]`, no un clúster distribuido.

## Flujo y datos compartidos

`prepare` copia únicamente código desde el proyecto del host, montado como `/source:ro`, a un volumen de trabajo `/project`. Genera nuevamente las fuentes con la semilla del proyecto y no copia los IDs de NiFi ni las capturas de la ejecución local. Cada despliegue conserva su propio estado.

NiFi, el coordinador y Spark comparten `/project/data`. Las carpetas de ingesta pertenecen al usuario 1000 de NiFi y permiten escritura compartida. La API monta código y datos de lectura; NiFi monta el código de lectura y los datos de escritura. El dashboard guarda su reporte de verificación en el volumen de trabajo separado del host. El coordinador mantiene un bloqueo de archivo para impedir dos lotes simultáneos.

```text
/project/data/source/                      API y CSV sintéticos
/project/data/inbox/catalog/               CSV recibidos por GetFile
/project/data/landing/events/base/         JSONL publicados por NiFi
/project/data/landing/events/delta/        Lote adicional separado
/project/data/landing/catalog/             Catálogo validado
/project/data/quality/ingesta/              Originales y motivos de rechazo
/project/data/manifests/base.json           Manifiesto cerrado con hashes
/project/data/manifests/delta.json
/project/data/processed/                    Salidas y resúmenes de Spark
/project/data/evidence/                     Evidencias de este despliegue
```

`run_batch.py` construye o reutiliza el flujo mediante `construir_flujo.py`; comprueba que los IDs pertenezcan a la instancia NiFi actual. Después llama a `ejecutar_lote.py`, que controla entradas, drena colas, captura métricas y sella el manifiesto. Spark recibe exclusivamente la carpeta del lote elegido y se ejecuta solo si el manifiesto declara cierre. El script no usa `--allow-unsealed`. El lote delta exige una publicación exitosa previa del lote base.

El driver JDBC 42.7.7 se descarga de PostgreSQL y se comprueba con el SHA256 del artefacto usado en el laboratorio local. Los repositorios de contenido, FlowFiles, estado, configuración y proveniencia de NiFi se conservan en volúmenes, al igual que PostgreSQL y los datos del proyecto.

## Comprobar el despliegue y conservar evidencias

```powershell
docker compose logs --tail 100 api nifi dashboard postgres
docker compose run --rm --entrypoint python pipeline /project/02_Codigo/pruebas/verify_database.py --output /project/data/evidence/database_verification.json
docker compose exec -w /project/03_Visualizaciones/dashboard dashboard python verify_integration.py
docker compose cp dashboard:/project/03_Visualizaciones/dashboard/verification_report.json ./docker_dashboard_verification.json
docker compose run --rm --entrypoint python pipeline /project/02_Codigo/procesamiento/benchmark.py --output /project/data/evidence/benchmark
docker compose cp api:/project/data/evidence ./docker_evidence
```

Los contenedores `run --rm` se eliminan al terminar. Para exportar las evidencias de NiFi desde su volumen sin depender de un contenedor batch existente:

```powershell
docker compose run --rm --entrypoint python pipeline -c "import shutil; shutil.copytree('/project/03_Visualizaciones/evidencias','/project/data/evidence/nifi',dirs_exist_ok=True)"
docker compose cp api:/project/data/evidence ./docker_evidence
```

Para un backup, PostgreSQL ya contiene las utilidades de su versión. Por ejemplo:

```powershell
docker compose exec postgres sh -c 'pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc -f /tmp/lcd301_backup.dump'
docker compose cp postgres:/tmp/lcd301_backup.dump ./lcd301_backup.dump
```

Este ejemplo guarda un respaldo; no prueba restauración. La prueba de respaldo/restauración del proyecto se documenta y ejecuta por separado. Conservar archivos de evidencias antes de eliminar volúmenes. `docker compose stop` detiene servicios conservando contenedores y volúmenes; `docker compose down` elimina contenedores y conserva volúmenes por defecto.

## Validación estática y límites

```powershell
python validate_static.py
docker compose config --quiet
```

El primer comando usa Python estándar y ya se ejecutó en la preparación. El segundo debe ejecutarse en el equipo con Docker: permanece pendiente. `compose.yaml` utiliza sintaxis JSON, compatible con YAML, para validar su estructura sin instalar un parser adicional.

La configuración fija versiones de aplicaciones y JDBC, pero las imágenes base y paquetes del sistema no están fijados por digest: no se afirma reproducibilidad binaria byte a byte. Tampoco se han verificado en Docker consumo de memoria, tiempos de arranque, permisos reales de los volúmenes o endpoints de salud. Repetir un lote exige comprobar idempotencia sobre las tablas persistidas; el proyecto conserva las comprobaciones locales y el despliegue permite repetir la prueba.
