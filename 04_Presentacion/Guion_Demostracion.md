# Guion de video siguiendo el manual de ejecución | Grupo A

Sigue el manual en este orden. En cada paso muestra lo que haces y explica su propósito. Las instalaciones se preparan antes de grabar.

**Importante:** la entrega ya tiene BASE y DELTA cargados. Puedes mostrar sus evidencias y repetir el procesamiento para comprobar que no duplica datos. Esa repetición conserva los resultados finales.

## 1. Preparar el entorno

Abre la carpeta del proyecto y muestra el README, las cuatro carpetas de entrega y `data/source`. Comprueba los requisitos del manual y activa el entorno virtual si lo utilizas.

**Explica:** «Analizamos ventas, devoluciones y clientes de una tienda con datos sintéticos. Usamos una API de eventos y un catálogo CSV».

## 2. Configurar las variables

Sigue los comandos del manual para definir `JAVA_HOME`, `NIFI_RUNTIME` y `DATABASE_URL`, adaptando las rutas. Trabaja desde la raíz del proyecto, donde está el README.

**Explica:** «Estas variables indican dónde están Java y NiFi y a qué base de datos nos conectamos».

## 3. Iniciar las aplicaciones

Comprueba que PostgreSQL y NiFi estén iniciados. Arranca API y dashboard siguiendo el manual, si todavía no están activos. Abre:

- NiFi: `https://localhost:8443/nifi/`.
- API: `http://127.0.0.1:18080/docs`.
- Dashboard: `http://127.0.0.1:18050/`.

**Explica:** «La API entrega eventos, NiFi los recibe y el dashboard muestra los resultados de PostgreSQL».

## 4. Crear el flujo propio

Ejecuta el constructor del manual y abre en NiFi el grupo `LCD301 | Grupo A | E-commerce nuevo`. Si ya existe, se reutiliza.

Señala las entradas REST y CSV, la validación, las colas y la salida de errores. Muestra un diagnóstico en `data/quality/ingesta/metadatos`.

**Explica:** «Este grupo reúne los bloques del proyecto. Los registros inválidos conservan su motivo de rechazo. Las colas limitan la acumulación y las solicitudes HTTP tienen reintentos».

## 5. Ingerir y sellar BASE

Sigue el comando de ingesta con `--batch-id base`. Muestra `data/landing/events/base` y `data/manifests/base.json`, cuyo estado debe ser `closed`.

**Explica:** «BASE es la carga inicial del historial. NiFi valida los datos y cierra el lote cuando está completo para que Spark pueda procesarlo».

Si aparece `already_closed`, el lote ya estaba terminado: muestra el manifiesto y continúa.

## 6. Procesar el lote cerrado

Sigue el comando Spark del manual, indicando la ruta correcta del JDBC. Después muestra `data/evidence/initial_run.json`, que conserva la primera ejecución.

**Explica:** «Spark limpia y normaliza los eventos, los une al catálogo, calcula agregados y aplica reglas antes de publicar en PostgreSQL».

Señala publicados, duplicados, rechazos y pendientes. La primera carga registrada publicó **31.000 eventos**. Ejecutar el job sobre los mismos datos comprueba la repetición; no publica otros 31.000.

## 7. Repetición y DELTA

Sigue la repetición de BASE y muestra `data/evidence/repeated_run.json`: publicó cero hechos nuevos.

Después sigue la ingesta y el procesamiento de DELTA del manual. Muestra `data/evidence/delta_run.json`.

**Explica:** «DELTA contiene operaciones posteriores: tres creaciones, tres pagos y una devolución. Añadió siete eventos y 280 BOB netos».

El acumulado pasó de **31.000 a 31.007 eventos**. Como DELTA ya está cargado, repetirlo mantiene ese total. Para mostrar el cambio original, utiliza las capturas `negocio_base.jpg` y `negocio_delta.jpg`, aclarando que son evidencias registradas.

## 8. Verificar resultados y respaldo

Sigue la verificación del manual; para la base final usa `--expected-phase after_delta`. Conserva los informes originales al guardar nuevas ejecuciones.

Muestra:

- `database_verification_after_delta.json`, en `data/evidence`: resultado `passed: true`.
- `02_Codigo/sql/00_schema.sql`: tablas y particiones mensuales.
- Los seis SQL de `02_Codigo/sql` y sus resultados y tiempos en `data/evidence/benchmark`.
- `data/evidence/backup_verification_after_delta.json`: resultado `verified: true` y comparación de las ocho tablas restauradas.

**Explica:** «Comprobamos conteos e importes, medimos las consultas y verificamos que el respaldo pudiera restaurarse».

## 9. Demostración final y entrega

Sigue el recorrido final del manual:

1. En **Negocio**, muestra los KPIs y los seis gráficos. Cambia canal o categoría.
2. Selecciona una categoría, un producto y un pedido para mostrar su historial.
3. En **Operación**, explica rendimiento, latencia, errores y colas. Si las métricas antiguas ya quedaron fuera de las últimas 24 horas, muestra `operacion_final.jpg` como evidencia registrada.
4. Cierra mostrando el README del repositorio y el ZIP de entrega.

**Cierre:** «Integramos dos fuentes con NiFi, procesamos con Spark, almacenamos en PostgreSQL y presentamos análisis y dashboards. El resultado final es de 31.007 eventos y 26.381.649,45 BOB netos, evitando duplicados al repetir los lotes».

Aclara que el procesamiento es batch, el panel consulta cada 15 segundos y Docker tuvo validación estática, sin ejecución en el laboratorio.
