# Guion de presentación y ejecución | LCD301 · Grupo A

Guía para un integrante del equipo que ejecuta el proyecto en **Windows con PowerShell**, con su propio usuario. Primero prepara el equipo; durante el video sigue los pasos 1 a 8 y utiliza las frases **Explica** como apoyo. No necesitas las carpetas privadas ni las credenciales del autor.

## Preparación antes de grabar — una sola vez

**Requisitos:** instala Python 3.12 con el lanzador `py`, Java JDK 21, Apache NiFi 2.12.0 y PostgreSQL 17. Spark 4.0.1 se instala con las dependencias Python. Los motores no vienen dentro del ZIP. Descargas oficiales: [Python](https://www.python.org/downloads/windows/), [Java 21](https://adoptium.net/temurin/releases/?version=21), [NiFi](https://nifi.apache.org/download/) y [PostgreSQL para Windows](https://www.postgresql.org/download/windows/). Versiones utilizadas y alcance: [DEPENDENCIAS.md](../02_Codigo/DEPENDENCIAS.md).

Usa una instalación local de NiFi reservada para esta demostración, sin un grupo anterior del proyecto, y deja activo el servicio de PostgreSQL desde **Servicios de Windows**. Necesitas conocer su puerto, usuario y contraseña; el usuario debe poder crear bases, también para la prueba de restauración. En una instalación de clase puedes usar el usuario `postgres` con la contraseña que elegiste al instalarlo. Los puertos 8443, 18080 y 18050 deben estar disponibles.

### A. Obtener el proyecto y preparar una copia limpia

En el [repositorio del equipo](https://github.com/SylentClyde/LCD301_ProyectoIntegrador_GrupoA), selecciona **Code → Download ZIP** y extráelo. También puedes usar el ZIP de entrega compartido por tu compañero. Localiza la carpeta que contiene `README.md`, `02_Codigo` y `data`. Copia su ruta desde el Explorador; cuando se soliciten rutas, pégalas **sin comillas exteriores**.

Abre PowerShell. Este bloque crea a su lado una carpeta nueva llamada `LCD301_Demo_GrupoA`. Copia únicamente código, dashboard y fuentes; así no reutiliza manifiestos, datos procesados ni identificadores NiFi de la entrega.

```powershell
$ErrorActionPreference = 'Stop'
$taskSource = (Resolve-Path -LiteralPath (Read-Host 'Ruta de la carpeta extraida que contiene README.md')).Path
if (!(Test-Path -LiteralPath "$taskSource/02_Codigo/requirements.txt")) { throw 'Selecciona la raiz del proyecto extraido.' }
$taskProject = Join-Path (Split-Path $taskSource -Parent) 'LCD301_Demo_GrupoA'
if (Test-Path -LiteralPath $taskProject) { throw 'LCD301_Demo_GrupoA ya existe. Conserva esa ejecucion o elige otro nombre para una demostracion nueva.' }
New-Item -ItemType Directory -Path "$taskProject/data", "$taskProject/03_Visualizaciones" -Force | Out-Null
Copy-Item -LiteralPath "$taskSource/02_Codigo" -Destination $taskProject -Recurse
Copy-Item -LiteralPath "$taskSource/data/source" -Destination "$taskProject/data" -Recurse
Copy-Item -LiteralPath "$taskSource/03_Visualizaciones/dashboard" -Destination "$taskProject/03_Visualizaciones" -Recurse
Set-Location -LiteralPath $taskProject
```

### B. Instalar dependencias y configurar las rutas propias

Cuando se soliciten las rutas, indica la carpeta **JDK que contiene `bin/java.exe`**, la carpeta **NiFi que contiene `bin/nifi.cmd`** y la carpeta **`bin` de PostgreSQL que contiene `psql.exe`**. Por ejemplo, esta última suele ser `C:/Program Files/PostgreSQL/17/bin`; utiliza la que exista en tu computadora.

```powershell
py -3.12 -m venv .venv
if ($LASTEXITCODE -ne 0) { throw 'Instala Python 3.12 con el lanzador py.' }
$taskPython = (Resolve-Path -LiteralPath '.venv/Scripts/python.exe').Path
& $taskPython -m pip install -r '02_Codigo/requirements.txt'
if ($LASTEXITCODE -ne 0) { throw 'Revisa la instalacion de dependencias antes de continuar.' }
$env:JAVA_HOME = (Resolve-Path -LiteralPath (Read-Host 'Ruta de la carpeta JDK 21')).Path
$env:NIFI_RUNTIME = (Resolve-Path -LiteralPath (Read-Host 'Ruta de la carpeta NiFi 2.12.0')).Path
$taskPgBin = (Resolve-Path -LiteralPath (Read-Host 'Ruta de la carpeta bin de PostgreSQL 17')).Path
foreach ($taskFile in @("$env:JAVA_HOME/bin/java.exe", "$env:NIFI_RUNTIME/bin/nifi.cmd", "$taskPgBin/psql.exe")) {
    if (!(Test-Path -LiteralPath $taskFile)) { throw "Revisa la ruta: $taskFile" }
}
$env:PATH = "$env:JAVA_HOME/bin;$env:PATH"
$env:PYTHONPATH = "$taskProject/02_Codigo"
$env:PYSPARK_PYTHON = $taskPython
$env:PROJECT_ROOT = $taskProject
$env:LCD301_PROJECT_ROOT = $taskProject
$env:DATA_ROOT = "$taskProject/data"
$env:SOURCE_DATA_DIR = "$taskProject/data/source"
$env:NIFI_URL = 'https://localhost:8443/nifi-api'
$env:DASH_HOST = '127.0.0.1'
$env:DASH_PORT = '18050'
New-Item -ItemType Directory -Path 'drivers', 'data/logs/presentacion' -Force | Out-Null
$taskJdbc = Join-Path $taskProject 'drivers/postgresql-42.7.7.jar'
Invoke-WebRequest 'https://jdbc.postgresql.org/download/postgresql-42.7.7.jar' -OutFile $taskJdbc
```

El último comando descarga el [driver JDBC oficial 42.7.7](https://jdbc.postgresql.org/download/) usado en el proyecto. La instalación puede tardar y requiere conexión a Internet. No necesitas activar el entorno Python: los comandos usan `$taskPython` directamente.

### C. Crear la base nueva y sus tablas

Utiliza el puerto y las credenciales **de tu instalación**, no las del equipo original. En una instalación estándar el puerto suele ser 5432. Este bloque crea `lcd301_grupoa_demo`; si ya existe, se detiene para evitar mezclar ejecuciones. No borra ninguna base.

```powershell
$taskPgPort = Read-Host 'Puerto PostgreSQL (Enter para 5432)'
if (!$taskPgPort) { $taskPgPort = '5432' }
$taskPgUser = Read-Host 'Usuario PostgreSQL (Enter para postgres)'
if (!$taskPgUser) { $taskPgUser = 'postgres' }
$taskPgSecret = Read-Host 'Contrasena de PostgreSQL' -AsSecureString
$taskPgPassword = ([System.Net.NetworkCredential]::new('', $taskPgSecret)).Password
$taskDatabase = 'lcd301_grupoa_demo'
$env:PGPASSWORD = $taskPgPassword
try {
    & "$taskPgBin/createdb.exe" -h 127.0.0.1 -p $taskPgPort -U $taskPgUser $taskDatabase
    if ($LASTEXITCODE -ne 0) { throw 'No se creo la base. Revisa servicio, puerto, credenciales y que el nombre no exista.' }
} finally { Remove-Item Env:PGPASSWORD -ErrorAction SilentlyContinue }
$taskDbUser = [uri]::EscapeDataString($taskPgUser)
$taskDbPassword = [uri]::EscapeDataString($taskPgPassword)
$env:DATABASE_URL = "postgresql://${taskDbUser}:${taskDbPassword}@127.0.0.1:${taskPgPort}/${taskDatabase}"
Remove-Variable taskPgPassword, taskPgSecret, taskDbPassword
& "$taskPgBin/psql.exe" -d $env:DATABASE_URL -v ON_ERROR_STOP=1 -f '02_Codigo/sql/00_schema.sql'
if ($LASTEXITCODE -ne 0) { throw 'Revisa la creacion de tablas antes de iniciar la ingesta.' }
```

**Conserva esta terminal abierta** para todos los pasos siguientes: contiene las rutas y la conexión, sin guardar la contraseña en el documento. La preparación se hace una vez por demostración nueva; para continuar una ejecución existente se reutilizan su carpeta y su base. No repitas la creación para volver a procesar BASE.

## 1. Presentar el proyecto

**Explica:** «Integramos ventas, devoluciones y clientes de una tienda. Recibimos eventos de una API REST y un catálogo CSV, validamos con NiFi, procesamos con Spark y almacenamos en PostgreSQL. Los dashboards presentan indicadores de negocio y operación. Los datos son sintéticos, en BOB».

La copia de demostración parte de cero. **BASE** es el historial inicial; **DELTA** es un lote posterior de operaciones nuevas. Al repetir un lote procesado se deben publicar cero hechos nuevos.

## 2. Iniciar y comprobar los servicios

Inicia NiFi solo si está detenido:

```powershell
& "$env:NIFI_RUNTIME/bin/nifi.cmd" start
```

Espera a que abra `https://localhost:8443/nifi/` e inicia sesión. En una instalación nueva con autenticación predeterminada, las credenciales generadas se consultan en `logs/nifi-app.log` dentro de la carpeta NiFi. Consúltalas antes de grabar. El cliente del proyecto las lee de ese mismo archivo. Si configuraste credenciales propias, indícalas en esta terminal:

```powershell
# Solo si sustituiste las credenciales generadas de NiFi:
$env:NIFI_USERNAME = Read-Host 'Usuario de NiFi'
$taskNifiSecret = Read-Host 'Contrasena de NiFi' -AsSecureString
$env:NIFI_PASSWORD = ([System.Net.NetworkCredential]::new('', $taskNifiSecret)).Password
Remove-Variable taskNifiSecret
```

La autenticación predeterminada se describe en la [guía oficial de NiFi](https://nifi.apache.org/nifi-docs/administration-guide.html#single-user). Si aparece una advertencia de certificado, comprueba que corresponde a tu NiFi local en `localhost`.

Inicia la API y el dashboard. Ejecuta cada bloque **solo si ese servicio no está ya activo para esta copia**. Si los puertos pertenecen a otra ejecución, detén esas aplicaciones antes de comenzar.

```powershell
Start-Process -FilePath $taskPython -ArgumentList @('-m','uvicorn','api.main:app','--host','127.0.0.1','--port','18080') -WorkingDirectory "$taskProject/02_Codigo" -WindowStyle Hidden -RedirectStandardOutput "$taskProject/data/logs/presentacion/api.log" -RedirectStandardError "$taskProject/data/logs/presentacion/api_error.log"
```

```powershell
Start-Process -FilePath $taskPython -ArgumentList @('-u','app.py') -WorkingDirectory "$taskProject/03_Visualizaciones/dashboard" -WindowStyle Hidden -RedirectStandardOutput "$taskProject/data/logs/presentacion/dashboard.log" -RedirectStandardError "$taskProject/data/logs/presentacion/dashboard_error.log"
```

Espera unos segundos y comprueba:

```powershell
Invoke-RestMethod 'http://127.0.0.1:18080/health'
Invoke-RestMethod 'http://127.0.0.1:18050/health'
```

**Muestra:** API con `source_available: true` y dashboard conectado a PostgreSQL. Antes de procesar BASE, la base nueva tiene cero eventos. Si un servicio no responde, revisa su archivo `*_error.log` en `data/logs/presentacion`.

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

En esta primera carga se esperan **31.000 eventos publicados**. Abre el dashboard y muestra los KPIs de BASE antes de continuar con DELTA. El job puede tardar; puedes recortar la espera conservando el resultado. Si da cero, comprueba si ya habías procesado este lote en la misma base.

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

**Explica:** «DELTA contiene operaciones posteriores: tres creaciones, tres pagos y una devolución. Su primera publicación añade siete eventos y 280 BOB netos».

Muestra el resumen de DELTA y espera la actualización del dashboard. En la copia nueva se esperan `published_count: 7` y estos cambios; si vuelves a ejecutarlo, debe publicar cero y conservar los totales:

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

**Resultado:** `verified: true`; compara ocho tablas e importes sin reemplazar la base principal. Muestra `data/logs/presentacion/backup/restore_verification.json`. La prueba crea una base adicional de restauración y requiere permiso para crear bases, como en la preparación.

## 8. Mostrar dashboards y cerrar

Abre `http://127.0.0.1:18050/`:

1. En **Negocio**, muestra los KPIs y los seis gráficos: líneas, barras, mapa de calor, dona, dispersión e histograma.
2. Cambia canal y categoría; espera la actualización. Después quita los filtros para recuperar los totales.
3. Selecciona una categoría, un producto y un pedido para mostrar su historial. La tabla muestra importes del producto; el historial, el pedido completo.
4. En **Operación**, señala rendimiento, latencia, errores, colas y ejecuciones recientes. Una repetición puede mostrar cero publicados por segundo: no creó hechos nuevos.

**Explica:** «El dashboard consulta PostgreSQL cada 15 segundos. La latencia se mide desde la marca individual en NiFi, después de SplitJson, hasta la persistencia e incluye la espera batch». Las métricas operacionales abarcan las últimas 24 horas: las repeticiones actuales no recrean las latencias de los eventos antiguos. En una ejecución nueva sí deben generarse observaciones actuales; si presentas otro día, explica la ventana. Las capturas históricas siguen en `03_Visualizaciones/capturas` de la carpeta original descargada, y deben identificarse como evidencia registrada.

**Cierre:** «Completamos ingesta, procesamiento, almacenamiento, análisis y visualización. La entrega final contiene 31.007 eventos y 26.381.649,45 BOB netos, con controles de calidad, repetición sin duplicados y respaldo restaurado».

Indica que se utilizó Spark batch local en Windows; Docker tuvo validación estática, sin ejecución en el laboratorio. Termina mostrando el README del repositorio y el ZIP `LCD301_ProyectoIntegrador_GrupoA.zip`. Los documentos y evidencias originales permanecen en la carpeta descargada (`$taskSource`); los resultados que acabas de ejecutar están en la copia de demostración (`$taskProject`).
