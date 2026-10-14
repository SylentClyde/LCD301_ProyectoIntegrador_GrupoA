# Guion de demostración: Grupo A

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
