# LCD301 · Proyecto integrador · Grupo A

E-commerce de ventas, devoluciones y clientes. Dos fuentes independientes (REST y catálogo CSV) atraviesan Apache NiFi, Apache Spark y PostgreSQL particionado. Dash/Plotly consulta los hechos publicados y muestra negocio y operación. Datos sintéticos en BOB, fechas de negocio UTC, semilla 301.

## Índice de entrega

Repositorio de entrega: [LCD301_ProyectoIntegrador_GrupoA](https://github.com/SylentClyde/LCD301_ProyectoIntegrador_GrupoA). El instructor puede consultar los archivos desde el navegador. [Descargar el ZIP con el nombre solicitado](https://github.com/SylentClyde/LCD301_ProyectoIntegrador_GrupoA/raw/refs/heads/main/LCD301_ProyectoIntegrador_GrupoA.zip) · [Descargar el acceso directo .url](https://github.com/SylentClyde/LCD301_ProyectoIntegrador_GrupoA/raw/refs/heads/main/LCD301_ProyectoIntegrador_GrupoA.url).

| Carpeta | Abrir primero |
|---|---|
| Documentación | [Informe técnico](01_Documentacion/Informe_Tecnico_GrupoA.pdf) · [Manual de ejecución](01_Documentacion/Manual_Ejecucion_GrupoA.pdf) |
| Código | [Dependencias](02_Codigo/DEPENDENCIAS.md) · [Fuentes](02_Codigo/fuentes/README.md) · [API](02_Codigo/api/README.md) · [Ingesta](02_Codigo/ingesta/README.md) · [Procesamiento](02_Codigo/procesamiento/README.md) |
| Almacenamiento y análisis | [Esquema SQL](02_Codigo/sql/00_schema.sql) · [Q1](02_Codigo/sql/Q1_ingresos_canal.sql) · [Q2](02_Codigo/sql/Q2_ranking_productos.sql) · [Q3](02_Codigo/sql/Q3_ticket_promedio.sql) · [Q4](02_Codigo/sql/Q4_cancelacion_cohortes.sql) · [Q5](02_Codigo/sql/Q5_devoluciones_30dias.sql) · [Q6](02_Codigo/sql/Q6_valor_repeticion_clientes.sql) |
| Visualización | [Dashboard](03_Visualizaciones/dashboard/README.md) · [Capturas reales](03_Visualizaciones/capturas/) · [Evidencias NiFi](03_Visualizaciones/evidencias/) |
| Presentación | [Slides](04_Presentacion/Entrega/Presentacion_GrupoA.pptx) · [Video](04_Presentacion/Demostracion_GrupoA.mp4) · [Guion](04_Presentacion/Guion_Demostracion.md) |
| Despliegue opcional | [Docker Compose y estado de validación](02_Codigo/deploy/README.md) |

## Qué comprobar

El lote BASE contiene 31.065 objetos: 31.000 eventos válidos, 25 duplicados y 40 eventos deliberadamente inválidos o pendientes. Veinte son rechazos estructurales de ingesta. Un segundo CSV inválido demuestra la validación de archivos. El lote DELTA contiene siete eventos nuevos.

| Indicador | BASE | Después de DELTA |
|---|---:|---:|
| Eventos publicados | 31.000 | 31.007 |
| Pedidos pagados | 12.800 | 12.803 |
| Ingreso bruto BOB | 28.041.488,60 | 28.041.868,60 |
| Reembolsos BOB | 1.660.119,15 | 1.660.219,15 |
| Ingreso neto BOB | 26.381.369,45 | 26.381.649,45 |

La evidencia de PostgreSQL debe coincidir con este cálculo independiente de la fuente. Repetir BASE debe publicar cero hechos nuevos. Consulte [estado de aceptación](ESTADO_VALIDACION.json), [validación inicial](data/evidence/database_verification_initial.json), [validación final](data/evidence/database_verification_after_delta.json), [backup restaurado](data/backups/restore_verification.json) y [rendimiento medido](data/evidence/benchmark/benchmark_summary.json). Las cifras son de un laboratorio académico y no representan una empresa real.

## Arranque nativo

Requisitos: Python 3.12, Java 21, NiFi 2.12.0, PostgreSQL 17.6 y JDBC PostgreSQL 42.7.7. Instalar las dependencias Python de `02_Codigo/requirements.txt`. Los motores externos y sus credenciales no se incluyen en el ZIP.

1. Crear la base/usuario de laboratorio; configurar `DATABASE_URL` y `JAVA_HOME`.
2. Iniciar NiFi y definir `NIFI_RUNTIME` o sus credenciales en variables de entorno.
3. Iniciar API y dashboard con `02_Codigo/iniciar_aplicaciones.ps1`.
4. Construir el grupo independiente con `02_Codigo/ingesta/construir_flujo.py --project-root .`.
5. Ejecutar la ingesta BASE con `02_Codigo/ingesta/ejecutar_lote.py --project-root . --batch-id base`.
6. Ejecutar Spark sobre `data/landing/events/base`, después de sellar el manifiesto. Usar los argumentos del [README de procesamiento](02_Codigo/procesamiento/README.md).
7. Repetir el job para probar idempotencia; luego ingresar/procesar DELTA desde su carpeta separada.

Servicios de laboratorio: [Dashboard](http://127.0.0.1:18050), [API y Swagger](http://127.0.0.1:18080/docs), [NiFi](https://localhost:8443/nifi/). Las direcciones localhost sirven únicamente en el equipo que ejecuta el proyecto.

## Alcance y entrega

El procesamiento es batch: las vistas se actualizan cada 15 segundos al publicarse los lotes. No se afirma procesamiento distribuido ni streaming continuo. El despliegue Docker se suministra como extra y no se ejecutó en este entorno; la evidencia es de ejecución nativa Windows. El video utiliza capturas reales como demostración guiada.

`MANIFEST_SHA256.json` inventaría todos los archivos distribuibles. La entrega se llama **LCD301_ProyectoIntegrador_GrupoA.zip**. Subir a OneDrive/Drive/Git y comprobar un enlace con permiso de visualización para el instructor. Una ruta dentro de OneDrive no demuestra por sí sola sincronización ni acceso externo.
