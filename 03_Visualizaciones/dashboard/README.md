# Dashboards de e-commerce · Grupo A

Aplicación Plotly Dash conectada únicamente a datos publicados en PostgreSQL. Tiene dos vistas, seis tipos de visualización y lectura automática cada 15 segundos. La actualización de negocio depende de cuándo NiFi y Spark publican un lote; un refresco visual no fuerza un nuevo procesamiento.

El período inicial abarca todas las fechas publicadas y sigue los nuevos límites cuando llegan lotes. Si se elige manualmente un rango interior, se conserva ese filtro. Las fechas que no cambian no se vuelven a emitir, para mantener la selección de categoría, producto y pedido durante cada refresco.

## Iniciar

Desde esta carpeta, con Python 3.11 o posterior y PostgreSQL inicializado:

```powershell
python -m pip install -r requirements.txt
$env:DATABASE_URL = 'postgresql://lcd301:lcd301_local@127.0.0.1:55432/lcd301'
python app.py
```

Abrir [el dashboard](http://127.0.0.1:18050). `DASH_HOST` y `DASH_PORT` permiten cambiar dirección y puerto. En Docker se usa `DASH_HOST=0.0.0.0` y una `DATABASE_URL` cuyo host sea el servicio PostgreSQL de Compose. Las credenciales anteriores son de laboratorio; se puede suministrar otra URL por variable de entorno.

La aplicación lee el esquema `ecommerce` creado por `02_Codigo/sql/00_schema.sql`. No carga CSV directamente ni inventa valores si faltan servicios. La comprobación [de salud](http://127.0.0.1:18050/health) devuelve HTTP 200 con el conteo real de eventos persistidos, o HTTP 503 si la base no está disponible.

## Negocio

Los filtros de fecha, canal y categoría afectan gráficos y KPI. Las fechas incluyen todo el día inicial y final, en UTC. Los importes se calculan en centavos y se presentan en BOB; no incluyen impuestos ni envío.

| Visualización | Medida |
| --- | --- |
| Líneas | Pagos menos reembolsos, por día de evento y canal |
| Barras | Unidades pagadas por categoría; detalle por producto |
| Mapa de calor | Pedidos pagados por hora y día de semana UTC |
| Dona | Participación de canales en pedidos pagados distintos |
| Dispersión | Frecuencia de pedidos e ingreso neto observado por cliente |
| Histograma | Importes pagados por pedido, agregando sus líneas una sola vez |

Las tarjetas añaden ingreso neto, pedidos pagados, importe reembolsado, proporción de clientes recurrentes, cancelación y devolución a 30 días. Recurrente significa al menos dos pedidos pagados en el período. No se presenta valor predictivo del cliente.

La cancelación usa la cohorte de pedidos creados en el período y el estado disponible en el corte de fuente mostrado arriba. La devolución a 30 días usa pagos del período con `pago + 30 días <= corte`, y solo reembolsos de esas líneas dentro de los 30 días siguientes al pago. El denominador son unidades pagadas de cohortes maduras. Una tasa sin denominador muestra «Sin datos».

Cuando se elige una categoría, el importe del ticket es el subtotal pagado de esa categoría por pedido; el conteo de pedidos sigue siendo distinto por ID. La tabla de un producto muestra los importes de ese producto en el período. El historial de un pedido incluye todos sus eventos y deja visible esta diferencia de alcance.

## Demostrar el drill-down

1. Seleccionar período, canal y categoría, o dejar todos los valores.
2. Hacer clic en una barra de **Ventas por categoría**.
3. Bajar a **Del resumen al pedido**; aparecen productos de la categoría elegida.
4. Hacer clic en un producto. La tabla muestra sus pedidos, hasta 200 ordenados por fecha reciente.
5. Seleccionar una celda del pedido; el historial muestra creación, pago, cancelación o reembolsos con ID de lote.
6. Cambiar un filtro o usar **Limpiar selección**. Se elimina el producto y pedido anteriores para evitar un detalle incoherente.

## Operación

Los filtros de negocio no cambian esta vista: usa las últimas 24 horas según el reloj de recepción, publicación y muestreo.

- Rendimiento del último job: `published_count / duration_seconds` de `pipeline_runs`, en **registros de negocio/s**.
- Serie de publicación: eventos persistidos por minuto divididos entre 60, en registros/s. Un FlowFile no equivale a un registro.
- Latencia: `persisted_at - ingested_at`, en milisegundos, desde el registro en NiFi hasta la persistencia. Incluye la espera batch y excluye la solicitud HTTP y la cola anterior a Split. Mediana, p95 e histograma se calculan sobre hasta 50.000 registros recientes; el tamaño utilizado queda visible.
- Calidad Spark: conteos en `quality_reject` por etapa. Errores NiFi: último snapshot `bulletin_errors` del colector, sin sumar repetidamente el mismo contador.
- Colas NiFi: muestras `system_metric`, métrica `queue_flowfiles`. Se conserva fecha de última muestra. Si no hay colector activo, se muestra «Sin lectura».
- Ejecuciones: lotes, estado, entradas, publicados, rechazados, pendientes, duplicados y duración. Los pendientes aguardan creación o pago antecedente y se reintentan; no generan ingresos ni se incluyen en barras de errores. El estado y los conteos provienen del intento registrado.

Las muestras de NiFi usan `source='nifi'` y etiquetas `group_id`, `batch_id`; las series pueden usar `labels.connection` si se instrumentan colas individuales. Las métricas requeridas proceden del colector y del procesador, nunca de un temporizador simulado del dashboard.

## Verificación

```powershell
python -m unittest discover -s . -p test_dashboard.py -v
```

Las pruebas comprueban el grano del ticket por pedido, un reembolso parcial, seis figuras, cambio y limpieza del drill-down, filtros SQL parametrizados, carga de la aplicación sin conexión y estado visible ante una caída de PostgreSQL. Los fixtures están aislados en el archivo de pruebas y no alimentan la interfaz.

Para evidencias del sistema integrado, capturar ambas vistas con el corte, conteo e ID de lote visibles. Registrar también una selección de categoría, producto y pedido y comprobar que una nueva publicación cambia el conteo real tras el próximo refresco.

Después de publicar un lote, `python verify_integration.py` comprueba el ingreso del gráfico contra los hechos financieros, seis tipos de gráfico, filtros, recorrido de detalle y consultas operacionales sobre la base real. Guarda `verification_report.json` con las observaciones, incluidos los IDs reales del recorrido. La disponibilidad de muestras de colas de NiFi queda registrada por separado: no se presume cuando el colector todavía no ha escrito muestras.

Referencias: [gráficos interactivos de Dash](https://dash.plotly.com/interactive-graphing), [actualización automática](https://dash.plotly.com/live-updates).
