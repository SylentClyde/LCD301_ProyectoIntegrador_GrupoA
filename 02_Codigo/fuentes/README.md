# Fuentes sintéticas — Grupo A

Generación reproducible, sin servicios externos ni datos personales. Semilla `301`, cien productos, mil clientes y fechas de abril a septiembre de 2026. Importes en centavos enteros BOB y fechas UTC.

Desde `02_Codigo`:

```powershell
python fuentes/generar_datos.py --project-root ..
```

La salida queda exclusivamente en `data/source/`; el generador no escribe en el directorio de llegada de Spark. NiFi debe transportar los eventos REST y el catálogo CSV.

| Archivo | Uso |
| --- | --- |
| `events_initial.json` | 31.000 eventos válidos, 40 casos inválidos y 25 duplicados exactos, desordenados de forma reproducible. |
| `events_delta.json` | Siete eventos adicionales únicos: tres compras y una devolución; incremento neto esperado de 280,00 BOB. |
| `catalogo_productos.csv` | Cien productos válidos, versión entera 1. |
| `catalogo_productos_invalido.csv` | Una fila con precio negativo, para demostrar rechazo del segundo origen. |
| `control_events.json` | Once eventos de cuatro pedidos con resultados calculados previamente. |
| `expected.json` | Conteos, importes, identificadores duplicados y motivos de los cuarenta casos de calidad. |
| `dataset_manifest.json` | Tamaños y SHA-256 de los archivos generados. |

Los eventos válidos incluyen 15.000 creaciones, 12.800 pagos, 1.200 cancelaciones y 2.000 devoluciones. Mil pedidos creados quedan abiertos sin pago ni cancelación. El pago reconoce ingresos; las creaciones y cancelaciones no los reconocen. La cancelación conserva las líneas originales y registra `total_cents=0`. Dos registros inválidos de devolución referencian un pago inexistente: el procesamiento puede dejarlos pendientes hasta resolver la referencia; nunca deben contribuir a ingresos.

`event.v1.schema.json` valida campos, tipos, moneda, canal, cantidades positivas y coherencia estructural de reembolsos. La integridad referencial, calendario real, importes de líneas, pago único y límites acumulados de devolución corresponden a Spark y PostgreSQL.

Pedidos de control:

| Pedido | Pago | Devolución | Neto | Condición |
| --- | ---: | ---: | ---: | --- |
| ORD-CONTROL-001 | 100,00 | 50,00 | 50,00 | Dos unidades vendidas; una devuelta. Creación en abril y pago en mayo. |
| ORD-CONTROL-002 | 85,00 | 0,00 | 85,00 | Tres unidades de 30,00 con descuento total de 5,00. Creación en mayo y pago en junio. |
| ORD-CONTROL-003 | 0,00 | 0,00 | 0,00 | Cancelación anterior a cualquier pago. |
| ORD-CONTROL-004 | 150,00 | 100,00 | 50,00 | Tres unidades; dos devoluciones parciales independientes de una unidad cada una. |

Los reembolsos aleatorios prorratean el importe neto original por cantidad devuelta y redondean hacia abajo a centavos. No se reemplaza el precio histórico por el precio actual del catálogo.
