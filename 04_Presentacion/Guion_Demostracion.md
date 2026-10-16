# Guion completo de exposición y demostración | Grupo A

Este documento indica qué preparar, qué abrir, qué hacer, qué explicar y qué comprobar para presentar el proyecto LCD301 de e-commerce: ventas, devoluciones y clientes. Duración orientativa: 12–15 minutos, recortando las esperas de ejecución. La consigna proporcionada no fija una duración; ajustar si el instructor estableció una.

Los comandos de este guion son para que los ejecute el expositor. La ampliación del documento no vuelve a ejecutar el sistema ni modifica los datos.

## 1. Entender el recorrido antes de grabar

**Lote** significa un conjunto de registros que se procesa junto. **BASE** es el lote inicial con el historial sintético. **DELTA** es un lote posterior de siete eventos nuevos: tres creaciones de pedidos, tres pagos y una devolución. Un evento es una acción; siete eventos no son siete ventas.

**Flujo propio** significa el grupo de procesadores y conexiones reservado para este trabajo en NiFi. Su nombre es `LCD301 | Grupo A | E-commerce nuevo`. El constructor crea el grupo si hace falta y reutiliza el existente cuando sus identificadores siguen siendo válidos. Se utiliza el mismo flujo para BASE y DELTA.

El recorrido de los datos es:

```text
API REST de eventos + archivo CSV de catálogo
                    ↓
NiFi recibe, valida, conserva errores y entrega archivos completos
                    ↓
Spark limpia, normaliza, enriquece, agrega y prepara la publicación
                    ↓
PostgreSQL valida el ciclo del pedido y almacena los hechos
                    ↓
Consultas SQL + dashboards de negocio y operación
```

NiFi y Spark tienen pasos separados: ejecutar la ingesta no equivale a haber publicado los datos en PostgreSQL. Primero se cierra el lote en NiFi; después se ejecuta el job Spark.

### Elegir cómo demostrarlo

**Recorrido recomendado con la entrega actual:** mostrar el sistema cargado, sus archivos de ejecución, los resultados y los dashboards. Se puede volver a ejecutar Spark para demostrar que no duplica eventos. Presentar las evidencias anteriores como ejecuciones registradas, indicando su fecha.

**Recorrido desde cero:** requiere un entorno de demostración separado, con otra base vacía y otra carpeta de datos que empiece únicamente con las fuentes, sin manifiestos ni archivos landing previos. Debe ensayarse antes de grabar. Este guion no incluye un comando de borrado o reinicio de la base entregada.

La entrega ya contiene BASE y DELTA ejecutados. Sobre esa base, repetir BASE no hace que el contador vuelva a 31.000; repetir DELTA no añade otros siete. Ambos deben conservar el acumulado de 31.007 eventos.

## 2. Preparación fuera de cámara

1. Descargar o localizar la carpeta del proyecto y abrir su `README.md`.
2. Abrir la presentación `04_Presentacion/Entrega/Presentacion_GrupoA.pptx`.
3. Preparar las pestañas de NiFi, API y dashboard; comprobar que los servicios funcionan antes de iniciar la grabación.
4. Dejar accesibles las carpetas `02_Codigo`, `data/evidence` y `03_Visualizaciones/capturas`.
5. Preparar una terminal PowerShell para los comandos elegidos. No es necesario ejecutar todas las pruebas y respaldos durante el video.
6. Ajustar el zoom para que se lean las cifras y el texto. Hacer una prueba de audio y una grabación corta.

### Preparar la terminal en el equipo original del laboratorio

Este bloque supone que siguen disponibles los motores y dependencias instalados en `tmp`. No se incluyen en el ZIP ni en GitHub. En otro equipo se deben instalar los requisitos del manual y adaptar las rutas; no copiar este bloque esperando que existan esas carpetas.

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
New-Item -ItemType Directory -Path 'data/logs' -Force | Out-Null
```

Las variables se aplican a esa terminal. Si se abre otra, repetir el bloque. En otro equipo con entorno virtual activado, usar su Python, el Java instalado y el JDBC correspondiente. La contraseña del ejemplo corresponde únicamente al laboratorio local.

### Comprobar servicios y abrir pantallas

| Servicio | Dirección o comprobación | Qué esperar |
| --- | --- | --- |
| NiFi | `https://localhost:8443/nifi/` | Acceso al lienzo y al grupo del proyecto |
| API | `http://127.0.0.1:18080/health` | `source_available: true` |
| Documentación API | `http://127.0.0.1:18080/docs` | Endpoints consultables |
| Dashboard | `http://127.0.0.1:18050/` | Estado conectado y cifras de PostgreSQL |
| Salud del dashboard | `http://127.0.0.1:18050/health` | Conteo persistido; 31.007 si está cargada la entrega final |

También se puede comprobar desde PowerShell:

```powershell
Invoke-RestMethod 'http://127.0.0.1:18080/health'
Invoke-RestMethod 'http://127.0.0.1:18050/health'
```

Si faltan servicios en el equipo original, iniciarlos antes de grabar. No arrancar una segunda copia sobre un puerto ocupado. Para PostgreSQL, comprobar primero:

```powershell
& "$taskPgBin/pg_ctl.exe" status -D "$taskRuntime/pgdata"
```

Solo si informa que el servidor está detenido y existe esa instalación:

```powershell
& "$taskPgBin/pg_ctl.exe" start -D "$taskRuntime/pgdata" -l "$taskRuntime/postgres_demo.log"
```

Solo si NiFi está detenido, usar su arranque instalado:

```powershell
& "$env:NIFI_RUNTIME/bin/nifi.cmd" start
```

Para iniciar la API o el dashboard que falte, usar una terminal preparada por aplicación. Esos procesos permanecen en ejecución; dejar sus terminales abiertas:

```powershell
# Terminal de API: ejecutar solo si no está activa.
& $taskPython -m uvicorn api.main:app --host 127.0.0.1 --port 18080
```

```powershell
# Otra terminal preparada: ejecutar solo si el dashboard no está activo.
& $taskPython "$taskProject/03_Visualizaciones/dashboard/app.py"
```

## 3. Inicio del video: problema y objetivo | 0:00–1:00

**Abrir:** diapositiva inicial y arquitectura.

**Hacer:** presentar al Grupo A, el caso de uso y las cinco capas.

**Explicar con tus palabras:**

> Una tienda recibe pedidos, pagos y devoluciones desde distintos canales. Nuestro proyecto integra esos eventos con un catálogo de productos, valida su calidad y calcula indicadores para analizar las ventas y el funcionamiento del sistema.

Indicar que los datos son sintéticos, la moneda es BOB y las fechas de negocio están en UTC. La implementación medida utiliza NiFi, Spark batch, PostgreSQL particionado y Dash/Plotly.

**Comprobar antes de avanzar:** que se entiendan las dos fuentes y el destino de los datos.

## 4. Mostrar fuentes y flujo NiFi | 1:00–3:00

### 4.1 Mostrar las dos fuentes

1. Abrir `http://127.0.0.1:18080/events?cursor=0&limit=2` para mostrar dos eventos sin cargar toda la fuente en pantalla.
2. Señalar `event_id`, `event_type`, `order_id`, `event_time`, `total_cents` e `items`.
3. Explicar que los importes se guardan en centavos enteros: `10000` centavos equivalen a `100,00 BOB`.
4. Mostrar las primeras filas del catálogo:

```powershell
Get-Content 'data/source/catalogo_productos.csv' -TotalCount 4
```

5. Señalar producto, categoría, estado, versión y precio. Distinguir el catálogo CSV de la API de eventos.

### 4.2 Mostrar el flujo propio

1. Abrir NiFi y entrar al grupo `LCD301 | Grupo A | E-commerce nuevo`.
2. Señalar la entrada REST y la entrada CSV.
3. Seguir las conexiones hacia validación, salida de archivos válidos y cuarentena.
4. Explicar que las conexiones conservan datos en cola y que el backpressure limita su acumulación. La configuración es de 1.000 FlowFiles o 100 MB por cola.
5. Explicar que un FlowFile puede contener muchos eventos; sus contadores no equivalen automáticamente a ventas o registros.
6. Mostrar `03_Visualizaciones/evidencias/nifi_backpressure.json` y `nifi_preflight_http.json` como pruebas registradas de presión y reintentos. El transporte admite tres reintentos adicionales al intento inicial.

Si hay que construir el grupo en una instancia preparada de NiFi, el comando es:

```powershell
& $taskPython '02_Codigo/ingesta/construir_flujo.py' --project-root $taskProject
```

En la instalación donde el grupo ya existe, el constructor lo reutiliza. No se debe dibujar otro flujo para cada lote. No ejecutar grupos ajenos al proyecto.

### 4.3 Mostrar calidad y errores

1. Abrir `data/quality/ingesta/metadatos` y elegir un diagnóstico JSON.
2. Señalar el origen y el motivo del rechazo.
3. Mostrar que se conserva el contenido rechazado en `data/quality/ingesta/originales`.
4. Explicar el caso del CSV con precio negativo y los eventos que incumplen el contrato.

**Resultado que debe quedar claro:** los datos inválidos se separan con un motivo y se conservan para revisión.

## 5. Explicar y demostrar BASE | 3:00–5:00

### Si se muestra la ejecución registrada, como en la entrega actual

1. Abrir `data/manifests/base.json` y señalar `status: closed`, conteos y huellas de archivos.
2. Mostrar `data/landing/events/base`: son los archivos que NiFi dejó listos para Spark.
3. Abrir `data/evidence/initial_run.json`; señalar entradas, publicados, rechazos, duplicados, pendientes y duración.
4. Mostrar `data/evidence/database_verification_initial.json` y su resultado `passed: true`.
5. Explicar que BASE envió 31.065 objetos por REST: NiFi rechazó 20 por estructura y entregó 31.045 ocurrencias. Spark/SQL publicaron 31.000 eventos, con 25 duplicados, 18 rechazos de negocio y dos pendientes sin antecedente. El CSV inválido también quedó rechazado.

Los 18 rechazos y los dos pendientes completan los 20 casos de negocio/antecedentes de la fuente; no sumarlos otra vez a los 40 casos deliberados totales. Los rechazos registrados por intento pueden reaparecer al repetir un job.

### Comandos de una carga inicial en un entorno preparado

Primero ingesta NiFi; después procesamiento Spark:

```powershell
& $taskPython '02_Codigo/ingesta/ejecutar_lote.py' --project-root $taskProject --batch-id base --db-url $env:DATABASE_URL
& $taskPython '02_Codigo/ingesta/verificar_lote.py' --project-root $taskProject --batch-id base
& $taskPython '02_Codigo/procesamiento/job.py' --project-root $taskProject --batch-id base --jdbc-jar $taskJdbc --report 'data/logs/grabacion_base.json'
```

Antes de usar esos comandos debe existir el esquema de PostgreSQL del proyecto; ver `02_Codigo/sql/00_schema.sql` y el manual. La carpeta de informes se prepara con:

```powershell
New-Item -ItemType Directory -Path 'data/logs' -Force | Out-Null
```

Prepararla antes de cualquier comando de este guion que escriba allí. `data/logs` queda fuera de la entrega y permite conservar los informes originales.

**En la entrega actual:** el ejecutor NiFi encuentra el manifiesto cerrado y devuelve `already_closed` después de comprobar las huellas. No vuelve a descargar BASE. Si luego se ejecuta Spark sobre esa base ya cargada, se comprueba una repetición, no una primera carga.

**Explicar Spark:** mostrar en `02_Codigo/procesamiento/job.py` la normalización de campos, la unión con el catálogo, las agregaciones y la salida de pagos de alto valor. Mostrar `daily_candidates.json`, `category_candidates.json` o `high_value_alerts.json` dentro de una carpeta de `data/processed`. Esos agregados candidatos preceden a la validación financiera; los KPIs finales proceden de los hechos publicados en PostgreSQL.

## 6. Repetición sin duplicados y lote DELTA | 5:00–6:30

### 6.1 Idempotencia: repetir sin duplicar

1. Abrir `data/evidence/repeated_run.json`.
2. Señalar que `published_count` es cero y que se detectan eventos ya vistos.
3. Explicar que el identificador único permite reconocer los registros existentes.

Para comprobarlo de nuevo en el entorno actual, se puede repetir el job de BASE del apartado anterior con otro nombre de informe, por ejemplo `data/logs/grabacion_repeticion.json`. Esto registra un nuevo intento y puede volver a registrar rechazos o reintentar pendientes; mantiene los hechos financieros existentes.

### 6.2 DELTA: operaciones posteriores

1. Abrir `http://127.0.0.1:18080/events/delta?cursor=0&limit=10` y mostrar sus siete eventos.
2. Mostrar `data/manifests/delta.json`, `data/evidence/delta_run.json` y `data/evidence/database_verification_after_delta.json`.
3. Comparar BASE y DELTA usando la siguiente tabla.

| Resultado | BASE | Después de DELTA |
| --- | ---: | ---: |
| Eventos publicados acumulados | 31.000 | 31.007 |
| Pedidos pagados | 12.800 | 12.803 |
| Ingreso bruto BOB | 28.041.488,60 | 28.041.868,60 |
| Reembolsos BOB | 1.660.119,15 | 1.660.219,15 |
| Ingreso neto BOB | 26.381.369,45 | 26.381.649,45 |

> Después del historial llegaron tres pedidos nuevos, sus tres pagos y una devolución. Los pagos sumaron 380 BOB y el reembolso fue de 100 BOB; el ingreso neto aumentó 280 BOB.

En un entorno que tiene BASE y todavía no tiene DELTA, los comandos son:

```powershell
& $taskPython '02_Codigo/ingesta/ejecutar_lote.py' --project-root $taskProject --batch-id delta --db-url $env:DATABASE_URL
& $taskPython '02_Codigo/ingesta/verificar_lote.py' --project-root $taskProject --batch-id delta
& $taskPython '02_Codigo/procesamiento/job.py' --project-root $taskProject --batch-id delta --jdbc-jar $taskJdbc --report 'data/logs/grabacion_delta.json'
```

Para mostrar el incremento en vivo: capturar primero el estado BASE, publicar DELTA, quitar filtros interiores que oculten sus fechas y esperar el refresco del panel. La lectura automática es cada 15 segundos, después de publicarse el lote. En la entrega ya cargada, mostrar las capturas `negocio_base.jpg`, `negocio_delta.jpg` y `actualizacion_automatica.txt` de `03_Visualizaciones/capturas`, identificándolas como evidencia registrada. Volver a procesar DELTA conserva los valores finales.

## 7. PostgreSQL, reglas y respaldo | 6:30–8:00

**Abrir:** `02_Codigo/sql/00_schema.sql` y el informe final de verificación.

**Hacer y explicar:**

1. Señalar dimensiones de productos/clientes y hechos de eventos/líneas.
2. Explicar las particiones mensuales y los índices según fecha, pedido y producto.
3. Señalar `event_registry`, que mantiene la identidad única global.
4. Explicar que el pago reconoce ingreso y la devolución lo resta. La creación y la cancelación no reconocen ingreso.
5. Explicar que una devolución requiere un pago original y no puede superar las cantidades ni el importe comprado. Los pendientes sin antecedente no generan ingreso.

Para mostrar tablas y particiones directamente, abrir una conexión PostgreSQL en un cliente SQL. En el laboratorio también se puede usar:

```powershell
& "$taskPgBin/psql.exe" -d $env:DATABASE_URL -c '\dt ecommerce.*'
& "$taskPgBin/psql.exe" -d $env:DATABASE_URL -c 'SELECT count(*) AS eventos FROM ecommerce.fact_event;'
```

Comprobación completa, sin reemplazar el informe histórico:

```powershell
& $taskPython '02_Codigo/pruebas/verify_database.py' --db-url $env:DATABASE_URL --expected 'data/source/expected.json' --expected-phase after_delta --require-idempotent --output 'data/logs/verificacion_grabacion.json'
```

El parámetro `after_delta` corresponde al conjunto final de 31.007 eventos. Usar `initial` solo en una base que realmente contenga BASE sin DELTA. `--require-idempotent` exige que ya exista una repetición exitosa que no haya publicado hechos nuevos.

**Backup:** abrir `data/evidence/backup_verification_after_delta.json`. Mostrar `verified: true`, las ocho tablas comparadas, sus conteos y hashes, y la base aislada de restauración. El respaldo final registrado ocupa 12.982.037 bytes y conserva los 31.007 hechos. Las métricas de `system_metric` no forman parte de la comparación de esas ocho tablas.

No hace falta restaurar durante la grabación. Si se quiere ejecutar otra comprobación, hacerlo previamente y guardar los nuevos respaldos separados:

```powershell
& $taskPython '02_Codigo/almacenamiento/backup_restore.py' --db-url $env:DATABASE_URL --pg-bin $taskPgBin --backup-dir 'data/logs/backup_grabacion'
```

El script crea una base nueva para restaurar y comparar. No reemplaza la base principal; requiere permiso de creación de bases y las utilidades de PostgreSQL.

## 8. Seis preguntas de negocio y rendimiento | 8:00–10:00

Abrir cada SQL de `02_Codigo/sql` y su archivo `_result.json` correspondiente en `data/evidence/benchmark`. Explicar la pregunta y señalar una fila o cifra del resultado, sin leer todas las filas.

| SQL | Pregunta que explicar | Resultado o interpretación que mostrar |
| --- | --- | --- |
| Q1 | ¿Cómo evoluciona el ingreso neto por fecha y canal? | Pagos menos reembolsos; comparar canales |
| Q2 | ¿Qué productos lideran la demanda por categoría? | Unidades vendidas y devueltas separadas, más importes |
| Q3 | ¿Cuál es el ticket promedio por canal? | Total pagado dividido entre pedidos pagados distintos |
| Q4 | ¿Qué proporción de pedidos se cancela? | Cancelaciones sin pago sobre creaciones de la misma cohorte |
| Q5 | ¿Qué productos tienen mayor devolución a 30 días? | Usar solo compras con 30 días completos de observación |
| Q6 | ¿Qué clientes repiten y qué valor aportan? | Frecuencia e ingreso neto observado; no CLV predictivo |

Después abrir `benchmark_summary.json` y un archivo `_plan.json`. Señalar primera medición, cinco repeticiones, mediana del cliente y plan EXPLAIN con tiempo del servidor. Las medianas finales registradas Q1–Q6 son 37,91 / 126,83 / 28,85 / 32,70 / 192,49 / 31,66 ms. No prometer que una nueva ejecución tendrá esos mismos tiempos.

Los SQL usan parámetros `start`, `end` y `as_of`; no pegar sus marcadores Python directamente en un editor SQL esperando que se sustituyan solos. El benchmark los suministra. Para volver a ejecutar las seis consultas sin reemplazar la evidencia original:

```powershell
& $taskPython '02_Codigo/procesamiento/benchmark.py' --db-url $env:DATABASE_URL --output 'data/logs/benchmark_grabacion' --start '2026-04-01T00:00:00Z' --end '2026-10-01T00:00:00Z' --as-of '2026-10-01T00:00:00Z'
```

Explicar que la primera medición no demuestra caché fría y que no se hizo una comparación antes/después de optimizar índices. Mostrar medidas reales del equipo, sin presentarlas como garantía de producción.

## 9. Dashboard de negocio e interactividad | 10:00–12:00

1. Abrir `http://127.0.0.1:18050/` y seleccionar **Negocio**.
2. Dejar todos los canales y categorías, con el período completo abril–septiembre de 2026. Confirmar 31.007 eventos persistidos y los KPIs finales.
3. Explicar ingreso neto, pedidos pagados e importe reembolsado. Mostrar también recurrencia, cancelación y devolución a 30 días.
4. Recorrer los seis tipos de gráfico: líneas, barras, mapa de calor, dona, dispersión e histograma. Explicar qué pregunta responde cada uno.
5. Elegir un canal y una categoría. Esperar a que terminen las consultas y señalar cómo cambian los valores.
6. Quitar los filtros para regresar al conjunto completo.
7. Hacer clic en una barra de **Ventas por categoría**.
8. Bajar a **Del resumen al pedido** y hacer clic en un producto.
9. Seleccionar un pedido de la tabla y mostrar su historial de eventos. Si tiene devolución, señalar su relación con el pago.
10. Usar **Limpiar selección** y comprobar que se puede comenzar otro recorrido.

La tabla del producto presenta importes de ese producto; el historial presenta el pedido completo. Explicar esa diferencia si los importes no coinciden. No garantizar que cualquier pedido elegido tenga una devolución: seleccionar uno que efectivamente la tenga.

El gráfico de dispersión muestra valor neto observado y frecuencia, no una predicción del cliente. La tasa de devolución usa cohortes maduras. El dashboard usa el último evento como corte de fuente; Q5 usa el corte fijo 1 de octubre UTC, por lo que sus denominadores pueden diferir.

## 10. Dashboard operacional | 12:00–13:00

1. Seleccionar **Operación**.
2. Mostrar rendimiento del último job, latencia mediana/p95 y última muestra de cola.
3. Recorrer publicación de registros, errores/rechazos, distribución de latencia, colas y tabla de ejecuciones.
4. Señalar una ejecución BASE, su repetición con cero publicados y DELTA cuando estén dentro del período consultado.
5. Explicar los pendientes como una categoría separada de los rechazos.

Definiciones para narrar correctamente:

- El rendimiento de la tarjeta es eventos publicados divididos entre duración del job. La repetición puede dar cero publicaciones por segundo: el sistema evitó duplicar eventos.
- La latencia va desde la marca individual de NiFi, después de SplitJson, hasta la persistencia; incluye validación, espera batch y Spark, y excluye la solicitud HTTP y colas anteriores a esa marca.
- Una cola vacía indica que no quedan FlowFiles esperando en la última muestra; no significa que no haya datos en PostgreSQL.
- La lectura del dashboard cada 15 segundos no convierte al pipeline batch en streaming continuo.

**Si se graba días después:** las métricas operacionales consultan una ventana reciente de 24 horas. Las series antiguas pueden no aparecer, la cola puede mostrar su última fecha o no haber lectura, y la latencia puede carecer de observaciones recientes. Explicar el período y usar `operacion_final.jpg` como captura fechada de la ejecución registrada. Una nueva repetición crea un intento actual, pero no vuelve a publicar los eventos originales para fabricar latencias nuevas. No cambiar relojes ni fechas de evidencia.

## 11. Pruebas, límites y cierre | 13:00–15:00

**Mostrar evidencias de pruebas:**

| Archivo | Qué demuestra |
| --- | --- |
| `data/evidence/transaction_test.json` | Fallo transaccional, rollback y repetición |
| `data/evidence/spark_fixture_test.json` | Spark real con JDBC y casos pequeños de control |
| `data/evidence/pending_conflict_test.json` | Conservación del evento pendiente y rechazo de variantes |
| `data/evidence/database_verification_after_delta.json` | Conciliación de hechos e importes contra la fuente |
| `03_Visualizaciones/evidencias/dashboard_verification_final.json` | Figuras, filtros, detalle y operación contra datos publicados |
| `data/evidence/backup_verification_after_delta.json` | Respaldo restaurado y comparación de ocho tablas |

Cerrar con tres resultados concretos: 31.007 eventos publicados, 26.381.649,45 BOB netos y repetición sin duplicar hechos. Explicar que se cumplieron las cinco capas de la consigna.

Indicar los límites que corresponden: datos sintéticos, ejecución local Windows y Spark `local[2]`; Docker Compose tiene validación estática y no se ejecutó en el laboratorio. No atribuir ML, despliegue distribuido, replicación o autenticación de API/Dash al proyecto.

Mostrar finalmente el README del repositorio y el enlace público: https://github.com/SylentClyde/LCD301_ProyectoIntegrador_GrupoA. Señalar las cuatro carpetas de entrega y el ZIP con nombre `LCD301_ProyectoIntegrador_GrupoA.zip`. El acceso directo para enviar al docente es `LCD301_ProyectoIntegrador_GrupoA.url`.

## 12. Lista de cobertura antes de entregar el video

- [ ] Se explica el problema de negocio y la arquitectura completa.
- [ ] Se muestran REST y CSV como dos fuentes diferentes.
- [ ] Se identifican validación, cuarentena, reintentos y backpressure en NiFi.
- [ ] Se explica qué significan BASE, DELTA y flujo propio.
- [ ] Se distinguen ingesta NiFi y procesamiento/publicación Spark.
- [ ] Se muestran limpieza, normalización, enriquecimiento, agregación y enrutamiento.
- [ ] Se explica pago, devolución parcial y control de duplicados/pendientes.
- [ ] Se muestran almacenamiento particionado y respaldo restaurado.
- [ ] Se explican seis consultas, sus resultados y tiempos medidos.
- [ ] Se presentan ambos dashboards, seis tipos de gráfico, filtros y detalle.
- [ ] Se explica la actualización BASE → DELTA con evidencia identificada o ejecución real.
- [ ] Se distingue evidencia histórica de lo que efectivamente se ejecutó durante la grabación.
- [ ] Se indican límites del laboratorio y el repositorio de entrega.
- [ ] Se reproduce el video completo para revisar audio, legibilidad y ausencia de contraseñas personales, tokens o sesiones de autenticación en pantalla.

## 13. Qué hacer si algo no coincide durante el ensayo

| Situación | Acción |
| --- | --- |
| NiFi devuelve `already_closed` | El lote ya está sellado; mostrar su manifiesto y evidencia. Es el comportamiento esperado de esta entrega. |
| Repetir DELTA no suma siete | Ya estaba publicado. Mostrar el control de duplicados y la comparación registrada BASE/DELTA. |
| La verificación `initial` falla con 31.007 | Se está contrastando la fase equivocada; utilizar `after_delta` para la base final. |
| Dashboard desconectado | Comprobar PostgreSQL y `DATABASE_URL`; corregir antes de grabar. |
| El panel no muestra los valores globales | Quitar filtros de canal/categoría, ampliar período y esperar a que actualice. |
| Operación no muestra latencias antiguas | Revisar la ventana de 24 horas y presentar las capturas históricas con su contexto. |
| Spark tarda uno o dos minutos | Explicar el arranque batch y recortar la espera manteniendo visible el resultado final. Los tiempos pueden variar. |
| Un comando no encuentra un archivo o módulo | Revisar carpeta actual, rutas y entorno de esa terminal; no instalar ni improvisar cambios durante la toma. |

## Anexo: recorrido abreviado de cinco minutos

Usar este resumen solo si el tiempo de exposición es corto. La preparación, las advertencias sobre lotes ya ejecutados y las definiciones de las secciones anteriores siguen aplicando.

Duración sugerida: cinco minutos. El recorrido presenta la arquitectura, las evidencias de ejecución y las capturas reales del dashboard.

| Tiempo | Pantalla | Explicación |
| --- | --- | --- |
| 00:00–00:30 | Arquitectura | Presentar API REST de eventos y catálogo CSV, NiFi, Spark batch, PostgreSQL y Dash. |
| 00:30–01:15 | NiFi | Mostrar ambos orígenes, lote inicial, cuarentena JSON y rechazo del CSV inválido. |
| 01:15–02:00 | Procesamiento y SQL | Mostrar la ejecución Spark y cotejar conteos e importes con expected.json. Mostrar particiones mensuales. |
| 02:00–02:40 | Calidad y repetición | Mostrar que la repetición no agrega hechos ni ingresos. Abrir un caso de devolución parcial. |
| 02:40–03:20 | Dashboard operacional | Mostrar filas publicadas, duración, throughput, latencia y rechazos. Diferenciar latencia de frescura de datos. |
| 03:20–04:15 | Dashboard de negocio | Aplicar filtro de canal y categoría. Abrir productos, pedidos y eventos del pedido. |
| 04:15–04:45 | Lote adicional | Mostrar la ingesta /events/delta y sus siete eventos nuevos, con 280,00 BOB adicionales netos. |
| 04:45–05:00 | Resultados | Presentar conciliación, backup restaurado y límites del laboratorio. |

Valores de control del lote inicial: 31.000 eventos válidos, 12.800 pagos, 28.041.488,60 BOB brutos, 1.660.119,15 BOB reembolsados y 26.381.369,45 BOB netos. El delta agrega 380,00 BOB brutos y 100,00 de devolución. Neto posterior: 26.381.649,45 BOB.

El laboratorio ejecutó NiFi 2.12.0, Spark 4.0.1 con `local[2]`, PostgreSQL 17.6 nativo de Windows y JDBC 42.7.7. Docker Compose cuenta con validación estática. Los contenedores no se ejecutaron y sus versiones no describen el entorno medido.

La calidad de BASE concilia 20 eventos rechazados por JSON Schema en NiFi, una fila inválida de CSV y 18 rechazos de negocio en Spark/SQL. Dos referencias de pago quedan pendientes y no afectan los ingresos. BASE contiene 25 duplicados exactos y la repetición publica cero hechos nuevos. La prueba de transporte conservó un fallo final tras cuatro intentos.

Al narrar la latencia, indicar su punto de inicio: `UpdateAttribute` de NiFi después de `SplitJson`. La medida hasta la persistencia incluye la espera del batch y Spark. Excluye el HTTP y las colas anteriores a ese punto.

El benchmark final corresponde a BASE + DELTA en PostgreSQL 17.6, con 31.007 hechos y 71.807 líneas. Cada una de las seis consultas conserva su primera medición y cinco repeticiones. Las medianas de tiempo cliente van de 28,85 a 192,49 ms. El dashboard estaba activo en el mismo host y el respaldo final había terminado antes de la medición. No hay aislamiento de CPU o disco. La primera medición no acredita caché fría y no hay comparación antes/después. Las mediciones de BASE permanecen en `data/evidence/benchmark_initial`.

El backup custom final ocupa 12.982.037 bytes, equivalentes a 12,98 MB decimales. La restauración en una base separada aprobó la comparación de conteos y SHA-256 de ocho tablas, además de los importes y pedidos pagados. El respaldo final conserva 31.007 hechos después del delta. El backup BASE se mantiene por separado.

La conciliación de BASE, repetición y DELTA aprobó contra PostgreSQL. La integración final del dashboard aprobó con los datos publicados. Las capturas finales muestran 31.007 eventos conectados, 12.803 pedidos pagados y 26.381.649,45 BOB de ingreso neto. Los datos son sintéticos y los resultados describen este laboratorio.
