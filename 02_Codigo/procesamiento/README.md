# Procesamiento y publicación | Grupo A

El job ejecuta Apache Spark batch real. Lee JSON/JSONL y CSV con esquemas explícitos, normaliza cadenas y fechas a UTC, valida las líneas y los importes, detecta IDs duplicados/conflictivos, une el catálogo sin perder SKU desconocidos, agrega candidatos por día/canal/categoría y separa pagos de alto valor. Escribe staging mediante el driver JDBC de PostgreSQL. Python coordina la publicación SQL; no reemplaza las transformaciones Spark.

## Ejecución

Requisitos: Python 3.12, Java 17 o 21, `pyspark==4.0.1`, `psycopg[binary]==3.2.10`, PostgreSQL y el driver JDBC 42.7.7. NiFi debe haber terminado el lote; `data/manifests/base.json` debe declarar `status: closed`. Los archivos completos en landing son inmutables; no ejecutar dos jobs simultáneos del mismo lote.

```powershell
python 02_Codigo/procesamiento/job.py --project-root . --batch-id base --jdbc-jar /ruta/postgresql-42.7.7.jar
python 02_Codigo/pruebas/test_business.py
python 02_Codigo/pruebas/test_transactions.py --output data/evidence/transaction_test.json
python 02_Codigo/pruebas/test_pending_conflicts.py --output data/evidence/pending_conflict_test.json
python 02_Codigo/pruebas/test_spark_pipeline.py --jdbc-jar /ruta/postgresql-42.7.7.jar --output data/evidence/spark_fixture_test.json
python 02_Codigo/pruebas/verify_database.py --output data/evidence/database_verification.json
python 02_Codigo/procesamiento/benchmark.py --output data/evidence/benchmark
python 02_Codigo/almacenamiento/backup_restore.py --pg-bin /ruta/pgsql/bin
```

`DATABASE_URL` define la conexión; valor de laboratorio: `postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301`. `DATA_ROOT` permite ubicar los datos en otro directorio. Por defecto se leen únicamente `data/landing/events/<batch-id>` y el catálogo compartido; `--events-dir` y `--catalog-dir` permiten rutas explícitas. `--allow-unsealed` existe únicamente para pruebas. El umbral de alertas predeterminado es 500.000 centavos (=5.000 BOB).

El laboratorio usa Spark `local[2]`. El driver JDBC se añade al classpath de conductor/ejecutor; el listado de nombres de archivo se obtiene en el conductor y los lectores Spark reciben rutas individuales. Así el pipeline funciona en Windows sin depender de binarios Hadoop adicionales. La prueba `test_spark_pipeline.py` ejercita lectores JSON/CSV, normalización, deduplicación, unión, rechazos y JDBC; usa una base aislada y confirma que el precio de compra no se reemplaza por el del catálogo.

### Comandos concretos del laboratorio registrado

Desde `Document Creation` (directorio que contiene `output` y `tmp`):

```powershell
$taskProject = (Resolve-Path 'output/LCD301_ProyectoIntegrador_GrupoA').Path
$taskRuntime = (Resolve-Path 'tmp/lcd301_runtime').Path
$taskPython = 'C:/Users/wolf1/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
$env:PYTHONPATH = "$taskRuntime/db_python;$taskRuntime/python"
$env:JAVA_HOME = (Resolve-Path 'tmp/nifi-runtime/jdk-21.0.12.1+1').Path
$env:PYSPARK_PYTHON = $taskPython
$env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
& $taskPython "$taskProject/02_Codigo/procesamiento/job.py" --project-root $taskProject --events-dir "$taskProject/data/landing/events/base" --batch-id base --jdbc-jar "$taskRuntime/postgresql-42.7.7.jar" --report "$taskProject/data/evidence/initial_run.json"
& $taskPython "$taskProject/02_Codigo/pruebas/verify_database.py" --expected "$taskProject/data/source/expected.json" --expected-phase initial --output "$taskProject/data/evidence/database_initial.json"
# Repetir exactamente el mismo job y comprobar que publicó cero hechos nuevos.
& $taskPython "$taskProject/02_Codigo/procesamiento/job.py" --project-root $taskProject --events-dir "$taskProject/data/landing/events/base" --batch-id base --jdbc-jar "$taskRuntime/postgresql-42.7.7.jar" --report "$taskProject/data/evidence/repeated_run.json"
& $taskPython "$taskProject/02_Codigo/pruebas/verify_database.py" --require-idempotent --expected "$taskProject/data/source/expected.json" --expected-phase initial --output "$taskProject/data/evidence/database_after_replay.json"
& $taskPython "$taskProject/02_Codigo/procesamiento/benchmark.py" --output "$taskProject/data/evidence/benchmark" --start 2026-04-01T00:00:00Z --end 2026-10-01T00:00:00Z --as-of 2026-10-01T00:00:00Z
& $taskPython "$taskProject/02_Codigo/almacenamiento/backup_restore.py" --pg-bin "$taskRuntime/pgsql/bin" --backup-dir "$taskProject/data/backups"
```

Para el incremento, esperar a que NiFi cierre `delta`, cambiar `--events-dir` a `data/landing/events/delta` y `--batch-id delta`, y verificar `--expected-phase after_delta`. El backup inicial queda identificado por fecha y SHA; después de publicar delta se puede generar una copia final. No se procesa un directorio donde NiFi siga escribiendo.

## Integridad de negocio

`event_registry` impone unicidad global; los hechos se particionan por primer día del mes UTC. Una transacción adquiere un bloqueo asesor para publicar registro, hechos, líneas, pendientes y contadores. Repetir un lote acepta cero hechos nuevos. Un ID publicado con contenido distinto se rechaza. Un pago exige creación previa, mismo cliente/canal/moneda e idénticas líneas/importes; solo existe un pago por pedido. La cancelación requiere pedido impago y tiene importe cero.

Los reembolsos identifican el pago y la línea original. Conservan nombre/categoría/versión/precio de la compra. La cantidad acumulada no supera lo comprado; el importe acumulado no supera `floor(importe_neto_original * cantidad_acumulada_devuelta / cantidad_original)`. Esto distribuye descuentos en centavos sin crear dinero por redondeo y permite que la última devolución recupere el centavo restante. Un pago o reembolso cuyo antecedente aún no llegó se conserva en `pending_event`, nunca genera ingreso y se reintenta en el siguiente job. Mientras espera, un ID conserva su primer contenido y marca de ingesta; las variantes se rechazan. Una prueba real aislada comprueba que la llegada posterior del antecedente publica la versión original. Los demás rechazos se preservan en `quality_reject` y archivos de cuarentena.

`fact_event` mantiene importes positivos originales: reconocer ingreso usa pago positivo y reembolso negativo. `fact_event_line` incorpora `quantity_signed` y `net_cents` con signos financieros; las líneas `created` tienen ambos en cero. Los agregados Spark se etiquetan `candidate_*`: se calculan antes de validar el ciclo y no son los KPIs publicados. Los dashboards consultan hechos publicados en PostgreSQL.

## Observabilidad, análisis y respaldo

`pipeline_runs` conserva cada intento, duración, throughput, filas, duplicados, pendientes y rechazos. La latencia es `persisted_at - ingested_at`, mediana y p95. NiFi fija `ingested_at` en UpdateAttribute «03 Registrar fuente y lote», después de SplitJson y antes de ValidateJson/Jolt/Merge: esta medida incluye validación individual, preparación del lote, espera del batch y Spark/publicación; excluye descarga HTTP y cola anterior a esa marca. Se usa mtime del archivo como fallback explícito si falta esa metadata. Los eventos históricos no se confunden con latencia del sistema: `event_time` no participa en este cálculo.

Las seis consultas están en `../sql/Q*.sql`; requieren parámetros `start`, `end` y `as_of`. Q5 usa solo pagos con 30 días completos de observación y cuenta devoluciones dentro de esos 30 días. El benchmark guarda primera ejecución, cinco repeticiones, mediana/min/max del cliente y `EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)` con tiempo servidor. La primera ejecución no garantiza caché fría; no se atribuyen mejoras a índices sin comparación controlada.

El backup usa `pg_dump -Fc --snapshot` y mantiene abierto un snapshot exportado de PostgreSQL para que el original comparado y el dump representen el mismo instante. Conserva siete copias creadas por el script y restaura en una base nueva, sin sobrescribir la principal. Compara conteos y SHA256 de todas las filas de ocho tablas, además de ingresos y pedidos. La prueba de fallo transaccional también usa una base separada y deja su resultado verificable.

## Resultado BASE ejecutado el 9 de octubre de 2026

- NiFi cerró 31.045 ocurrencias válidas para continuar, procedentes de 31.065 eventos REST; rechazó 20 eventos en ingesta y una fila CSV. El catálogo publicado contiene 100 productos.
- Spark publicó 31.000 eventos en 96,242 segundos: 25 duplicados, 18 rechazos de Spark/negocio y dos reembolsos pendientes sin pago antecedente. Los dos pendientes no generan hechos ni ingreso. Throughput del job: 322,572 registros/s.
- Latencia desde la marca NiFi a persistencia: mediana 674.110,78 ms; p95 762.390,34 ms. Estos valores incluyen la espera y preparación del batch registrado y no describen una optimización de latencia.
- Repetir los mismos archivos publicó cero eventos nuevos y detectó 31.025 duplicados (31.000 ya registrados y 25 repetidos dentro de la entrada). El ingreso neto permaneció en 26.381.369,45 BOB; se conservaron 12.800 pagos, 79.442 unidades pagadas, 4.732 unidades devueltas y 1.000 clientes pagadores, de los que 795 repiten.
- La verificación independiente pasó 12 controles sobre el conjunto completo, además de 12 pruebas unitarias de negocio, la prueba de rollback y la prueba real pequeña Spark/JDBC.
- PostgreSQL contiene seis particiones mensuales en cada tabla de hechos; 31.000 eventos y 71.800 líneas. Las seis consultas guardan resultados y planes reales; medianas del cliente: 40,84 / 122,51 / 38,44 / 36,97 / 219,66 / 40,16 ms. Las mediciones coexistieron con el backup en el mismo host.
- Backup BASE: `lcd301_20261009T234038936774Z.dump`, 12.979.945 bytes. SHA256 `2aaa5643d54daa00b729335994e6a63fbbd94bfcd1daa8d175b067834d1f744b`. Restauración en `lcd301_restore_20261009_234038936774` verificada: conteos y hashes exactos de ocho tablas, más ingreso/pedidos.

Las evidencias originales son `data/evidence/initial_run.json`, `repeated_run.json`, `database_verification_initial.json`, `benchmark_initial/benchmark_summary.json`, `transaction_test.json`, `spark_fixture_test.json` y `backup_verification_initial.json`. Los rechazos se cuentan por intento; el replay vuelve a registrar 18 intentos de rechazo sin crear eventos financieros duplicados. La actualización DELTA se documentará por separado para preservar esta línea base.

## Resultado final después de DELTA

NiFi cerró el lote incremental de siete eventos sin rechazo. El job del 9 de octubre de 2026 publicó exactamente los siete: tres creaciones, tres pagos y una devolución. El acumulado final es 31.007 eventos y 71.807 líneas; 12.803 pagos, 2.001 devoluciones, 79.452 unidades pagadas, 4.734 unidades devueltas y 26.381.649,45 BOB de ingreso neto. El incremento neto es 280,00 BOB. Los 1.000 clientes pagadores y 795 recurrentes no cambiaron.

DELTA tardó 79,561 segundos y procesó 0,088 registros/s; este lote pequeño incluye el coste de iniciar Spark. La latencia desde la marca de ingesta NiFi a persistencia fue 146.048,641 ms de mediana y 146.050,183 ms p95. `pending_count=2` representa el backlog BASE reintentado, sin nuevos hechos ni ingreso, y no son dos entradas adicionales del lote de siete.

Los 12 controles completos pasaron en `data/evidence/database_verification_after_delta.json`; `delta_run.json` conserva la ejecución. La prueba `pending_conflict_test.json` verifica en una base aislada que un ID pendiente conserva su contenido y primera marca de ingesta, rechaza variantes y se publica correctamente cuando llega su antecedente.

`data/evidence/benchmark/benchmark_summary.json` contiene el benchmark FINAL, con 31.007 eventos y 71.807 líneas. Medianas cliente Q1–Q6: 37,912 / 126,829 / 28,853 / 32,697 / 192,486 / 31,659 ms. Son cinco repeticiones por consulta, más una primera ejecución y un plan instrumentado separado. El backup final había terminado; el dashboard y el host compartido permanecían activos. La caché y la carga no se aislaron, por lo que estos valores no demuestran mejoras respecto a BASE.

El backup FINAL `lcd301_20261009T234900595099Z.dump` mide 12.982.037 bytes y tiene SHA256 `c6757a5898f45cf793ac3f1e55cb0d13c5867f4485ffe8ce6a090310c6cf3319`. La restauración en `lcd301_restore_20261009_234900595099` coincide exactamente con el snapshot original en las ocho tablas y los totales financieros. Evidencia: `data/evidence/backup_verification_after_delta.json`; se conserva el respaldo BASE y su informe independiente.

Para reproducir el incremento después del cierre NiFi:

```powershell
& $taskPython "$taskProject/02_Codigo/procesamiento/job.py" --project-root $taskProject --batch-id delta --jdbc-jar "$taskRuntime/postgresql-42.7.7.jar" --report "$taskProject/data/evidence/delta_run.json"
& $taskPython "$taskProject/02_Codigo/pruebas/verify_database.py" --require-idempotent --expected "$taskProject/data/source/expected.json" --expected-phase after_delta --output "$taskProject/data/evidence/database_verification_after_delta.json"
& $taskPython "$taskProject/02_Codigo/almacenamiento/backup_restore.py" --pg-bin "$taskRuntime/pgsql/bin" --backup-dir "$taskProject/data/backups"
& $taskPython "$taskProject/02_Codigo/procesamiento/benchmark.py" --output "$taskProject/data/evidence/benchmark" --start 2026-04-01T00:00:00Z --end 2026-10-01T00:00:00Z --as-of 2026-10-01T00:00:00Z
```

En una base ya publicada, repetir estos comandos mantiene los importes, pero crea un nuevo intento idempotente. Los informes de ejecución con nombres fijos se reemplazan por el nuevo intento; las copias de backup y sus informes con fecha se conservan según la retención de siete dumps.

### Verificación del filtro de cancelaciones del dashboard

La consulta de cancelaciones filtra los eventos únicos de creación en `event_registry` antes de agregar y une los pagos/cancelaciones mediante los índices parciales existentes. Así evita que una estimación de una fila en la vista agregada produzca una búsqueda repetida de las líneas por cada pedido. La categoría selecciona pedidos que tenían una línea de esa categoría al crearse; cada pedido cuenta una vez.

`data/evidence/dashboard_performance` conserva el plan estimado original y los planes estimado/real de la consulta corregida. Los 15 pares de canal/categoría dieron conteos idénticos a un cálculo independiente basado en la vista original: máximo observado del cliente 106,066 ms; ejecución instrumentada de un plan 239,280 ms. El límite de la aplicación sigue siendo 12 segundos. No se modificaron datos ni índices. La integración completa de figuras, filtros, navegación y operación pasó con 31.007 eventos en `data/evidence/dashboard_integration_final.json`, junto con las ocho pruebas del dashboard.
