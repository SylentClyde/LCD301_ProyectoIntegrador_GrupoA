# Guion de presentación y ejecución | LCD301 · Grupo A

Ejecuta los bloques en orden y utiliza las frases **Explica** como apoyo. Los comandos están preparados para el equipo donde construimos el proyecto; las instalaciones se dejan listas antes de grabar.

**Situación actual:** BASE y DELTA ya están publicados. Al repetir la ingesta puede aparecer `already_closed` y Spark debe publicar cero hechos nuevos. Eso demuestra el control de duplicados. Para mostrar por primera vez el aumento de 31.000 a 31.007 se necesita otro entorno de demostración vacío; este recorrido conserva la base entregada.

## 1. Presentar el proyecto y preparar la terminal

**Explica:** «Integramos ventas, devoluciones y clientes de una tienda. Recibimos eventos de una API REST y un catálogo CSV, validamos con NiFi, procesamos con Spark y almacenamos en PostgreSQL. Los dashboards presentan indicadores de negocio y operación. Los datos son sintéticos, en BOB».

Abre PowerShell y ejecuta este bloque una vez. Las rutas `tmp` pertenecen a la instalación local y no se incluyen en el repositorio; en otro equipo hay que adaptar las rutas a los motores y dependencias instalados.

```powershell
Set-Location 'C:/Users/wolf1/OneDrive/Documents/ChatGPT/Document Creation'
$taskWorkspace = (Get-Location).Path
$taskProject = (Resolve-Path 'output/LCD301_ProyectoIntegrador_GrupoA').Path
$taskRuntime = (Resolve-Path 'tmp/lcd301_runtime').Path
$taskPython = 'C:/Users/wolf1/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/python.exe'
$taskJdbc = Join-Path $taskRuntime 'postgresql-42.7.7.jar'
$taskPgBin = Join-Path $taskRuntime 'pgsql/bin'
$env:JAVA_HOME = (Resolve-Path 'tmp/nifi-runtime/jdk-21.0.12.1+1').Path
$env:NIFI_RUNTIME = (Resolve-Path 'tmp/nifi-runtime/nifi-2.12.0').Path
$env:PYTHONPATH = "$taskRuntime/db_python;$taskRuntime/python;$taskProject/02_Codigo"
$env:PYSPARK_PYTHON = $taskPython
$env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
$env:DATABASE_URL = 'postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301'
$env:PROJECT_ROOT = $taskProject
$env:LCD301_PROJECT_ROOT = $taskProject
$env:DASH_HOST = '127.0.0.1'
$env:DASH_PORT = '18050'
Set-Location $taskProject
New-Item -ItemType Directory -Path 'data/logs/presentacion' -Force | Out-Null
```

**Explica:** «Configuramos las rutas de los programas y la conexión de laboratorio. Los informes nuevos se guardarán aparte, conservando las evidencias originales».

Mantén esta terminal abierta. Si abres otra para ejecutar estos comandos, repite la preparación.

## 2. Iniciar y comprobar los servicios

Comprueba PostgreSQL:

```powershell
& "$taskPgBin/pg_ctl.exe" status -D "$taskRuntime/pgdata"
```

Solo si PostgreSQL está detenido, ejecuta:

```powershell
& "$taskPgBin/pg_ctl.exe" start -D "$taskRuntime/pgdata" -l "$taskRuntime/postgres_presentacion.log"
```

Abre `https://localhost:8443/nifi/`. Solo si NiFi está detenido, inícialo y espera a que termine de arrancar:

```powershell
& "$env:NIFI_RUNTIME/bin/nifi.cmd" start
```

Abre `http://127.0.0.1:18080/health` y `http://127.0.0.1:18050/`. Si la API o el dashboard ya responden, omite su arranque. Para iniciar únicamente la aplicación que falte:

```powershell
# Solo si la API no está activa:
Start-Process -FilePath $taskPython -ArgumentList @('-m','uvicorn','api.main:app','--host','127.0.0.1','--port','18080') -WorkingDirectory "$taskProject/02_Codigo" -WindowStyle Hidden
```

```powershell
# Solo si el dashboard no está activo:
Start-Process -FilePath $taskPython -ArgumentList @('-u','app.py') -WorkingDirectory "$taskProject/03_Visualizaciones/dashboard" -WindowStyle Hidden
```

Comprueba que estén disponibles:

```powershell
Invoke-RestMethod 'http://127.0.0.1:18080/health'
Invoke-RestMethod 'http://127.0.0.1:18050/health'
```

**Muestra:** API con `source_available: true` y dashboard conectado a PostgreSQL. La base final debe tener 31.007 eventos.

## 3. Mostrar las fuentes y el flujo propio

Abre `http://127.0.0.1:18080/events?cursor=0&limit=2` y muestra el catálogo:

```powershell
Get-Content 'data/source/catalogo_productos.csv' -TotalCount 4
```

**Explica:** «La API entrega acciones de los pedidos y el CSV aporta los productos y sus categorías. Los importes se guardan en centavos enteros».

Construye o recupera el grupo del proyecto:

```powershell
& $taskPython '02_Codigo/ingesta/construir_flujo.py' --project-root $taskProject
```

**Muestra en NiFi:** el grupo `LCD301 | Grupo A | E-commerce nuevo`, las dos entradas, validación, colas y ruta de rechazos.

**Explica:** «Flujo propio es el grupo reservado para este proyecto. Se reutiliza para ambos lotes. La validación separa errores; las colas tienen backpressure de 1.000 FlowFiles o 100 MB y las solicitudes HTTP admiten tres reintentos adicionales».

## 4. Ingerir y cerrar BASE

**Explica:** «BASE es la carga inicial del historial. NiFi debe completar la entrega antes de que Spark la procese».

```powershell
& $taskPython '02_Codigo/ingesta/ejecutar_lote.py' --project-root $taskProject --batch-id base --db-url $env:DATABASE_URL
& $taskPython '02_Codigo/ingesta/verificar_lote.py' --project-root $taskProject --batch-id base
```

**Resultado:** manifiesto cerrado y verificación aprobada. `already_closed` significa que el lote ya estaba completo y se comprobaron sus archivos, sin volver a descargarlo.

**Muestra:** `data/manifests/base.json`, `data/landing/events/base` y un diagnóstico de `data/quality/ingesta/metadatos`. Explica que los rechazos conservan contenido y motivo; la ejecución registrada rechazó 20 eventos por estructura y un CSV inválido.

## 5. Procesar BASE con Spark

```powershell
& $taskPython '02_Codigo/procesamiento/job.py' --project-root $taskProject --batch-id base --jdbc-jar $taskJdbc --report 'data/logs/presentacion/base.json'
Get-Content 'data/logs/presentacion/base.json'
```

**Explica:** «Spark limpia y normaliza, une el catálogo, calcula agregados y separa registros de calidad y pagos de alto valor. La publicación valida las reglas financieras en una transacción de PostgreSQL».

**Señala en el resumen:** `input_count`, `published_count`, `duplicate_count`, `rejected_count`, `pending_count` y `duration_seconds`.

En una primera carga se publican 31.000 eventos. En esta base ya cargada se esperan **cero publicaciones nuevas**. Para mostrar el resultado original abre `data/evidence/initial_run.json`, identificándolo como ejecución registrada. El job puede tardar; puedes recortar la espera conservando el resultado.

## 6. Repetir BASE y procesar DELTA

Repite el mismo lote y guarda otro informe:

```powershell
& $taskPython '02_Codigo/procesamiento/job.py' --project-root $taskProject --batch-id base --jdbc-jar $taskJdbc --report 'data/logs/presentacion/repeticion.json'
Get-Content 'data/logs/presentacion/repeticion.json'
```

**Resultado:** `published_count: 0`. **Explica:** «La repetición reconoce los identificadores existentes y evita duplicar los hechos. Puede registrar nuevos intentos de rechazo y reintentar pendientes».

Abre `http://127.0.0.1:18080/events/delta?cursor=0&limit=10` y ejecuta:

```powershell
& $taskPython '02_Codigo/ingesta/ejecutar_lote.py' --project-root $taskProject --batch-id delta --db-url $env:DATABASE_URL
& $taskPython '02_Codigo/ingesta/verificar_lote.py' --project-root $taskProject --batch-id delta
& $taskPython '02_Codigo/procesamiento/job.py' --project-root $taskProject --batch-id delta --jdbc-jar $taskJdbc --report 'data/logs/presentacion/delta.json'
Get-Content 'data/logs/presentacion/delta.json'
```

**Explica:** «DELTA contiene operaciones posteriores: tres creaciones, tres pagos y una devolución. Su primera publicación añadió siete eventos y 280 BOB netos».

Como DELTA ya está publicado, esta repetición también debe conservar el total. Muestra `data/evidence/delta_run.json` o las capturas `negocio_base.jpg` y `negocio_delta.jpg` para explicar el cambio original:

| Indicador | BASE | Después de DELTA |
| --- | ---: | ---: |
| Eventos acumulados | 31.000 | 31.007 |
| Pedidos pagados | 12.800 | 12.803 |
| Ingreso neto BOB | 26.381.369,45 | 26.381.649,45 |

## 7. Verificar almacenamiento, consultas y respaldo

Ejecuta la verificación final:

```powershell
& $taskPython '02_Codigo/pruebas/verify_database.py' --db-url $env:DATABASE_URL --expected 'data/source/expected.json' --expected-phase after_delta --require-idempotent --output 'data/logs/presentacion/verificacion.json'
```

**Resultado:** `passed: true`. **Explica:** «Los conteos e importes coinciden con un cálculo independiente de la fuente. El pago suma ingreso, la devolución lo resta y los pendientes sin antecedente no generan ingreso».

Muestra las tablas y particiones mensuales:

```powershell
& "$taskPgBin/psql.exe" -d $env:DATABASE_URL -c '\dt ecommerce.*'
```

Ejecuta las seis consultas y muestra los tiempos obtenidos:

```powershell
& $taskPython '02_Codigo/procesamiento/benchmark.py' --db-url $env:DATABASE_URL --output 'data/logs/presentacion/benchmark' --start '2026-04-01T00:00:00Z' --end '2026-10-01T00:00:00Z' --as-of '2026-10-01T00:00:00Z'
(Get-Content 'data/logs/presentacion/benchmark/benchmark_summary.json' -Raw | ConvertFrom-Json).queries | Select-Object query,result_rows,median_client_ms
```

**Explica las preguntas:** ingresos por canal, productos líderes, ticket promedio, cancelaciones, devoluciones a 30 días y valor/repetición de clientes. Abre un SQL en `02_Codigo/sql` y su resultado en la carpeta del benchmark recién creado. Cada consulta tiene una primera medición y cinco repeticiones; los tiempos pueden variar.

Para comprobar otro respaldo y restauración en una base separada:

```powershell
& $taskPython '02_Codigo/almacenamiento/backup_restore.py' --db-url $env:DATABASE_URL --pg-bin $taskPgBin --backup-dir 'data/logs/presentacion/backup'
```

**Resultado:** `verified: true`; compara ocho tablas e importes sin reemplazar la base principal. Puedes preparar esta ejecución antes de grabar y mostrar `data/logs/presentacion/backup/restore_verification.json`, o utilizar la evidencia original `data/evidence/backup_verification_after_delta.json` indicando su fecha.

## 8. Mostrar dashboards y cerrar

Abre `http://127.0.0.1:18050/`:

1. En **Negocio**, muestra los KPIs y los seis gráficos: líneas, barras, mapa de calor, dona, dispersión e histograma.
2. Cambia canal y categoría; espera la actualización. Después quita los filtros para recuperar los totales.
3. Selecciona una categoría, un producto y un pedido para mostrar su historial. La tabla muestra importes del producto; el historial, el pedido completo.
4. En **Operación**, señala rendimiento, latencia, errores, colas y ejecuciones recientes. Una repetición puede mostrar cero publicados por segundo: no creó hechos nuevos.

**Explica:** «El dashboard consulta PostgreSQL cada 15 segundos. La latencia se mide desde la marca individual en NiFi, después de SplitJson, hasta la persistencia e incluye la espera batch». Las métricas operacionales abarcan las últimas 24 horas: las repeticiones actuales no recrean las latencias de los eventos antiguos. Si faltan observaciones, explica la ventana y muestra `03_Visualizaciones/capturas/operacion_final.jpg` como evidencia registrada.

**Cierre:** «Completamos ingesta, procesamiento, almacenamiento, análisis y visualización. La entrega final contiene 31.007 eventos y 26.381.649,45 BOB netos, con controles de calidad, repetición sin duplicados y respaldo restaurado».

Indica que se utilizó Spark batch local en Windows; Docker tuvo validación estática, sin ejecución en el laboratorio. Termina mostrando el README del repositorio y el ZIP `LCD301_ProyectoIntegrador_GrupoA.zip`.
