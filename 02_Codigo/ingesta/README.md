# Ingesta real de Grupo A

`construir_flujo.py` crea un grupo nuevo y exporta el flujo nativo para NiFi2.12.0. No modifica otros grupos. Si los IDs pertenecen a otra instalación (404), construye un grupo nuevo y conserva los IDs anteriores como evidencia.

Fuentes: API REST sintética de ventas/devoluciones y CSV de catálogo. `ValidateJson` comprueba el contrato antes del enriquecimiento; Groovy valida filas y claves de catálogo. Los rechazos conservan el original y un diagnóstico. `RetryFlowFile` admite3 reintentos HTTP y envía los fallos definitivos a cuarentena. Cada cola tiene backpressure de1000 FlowFiles o100MB, sin expiración.

```text
python construir_flujo.py --project-root <proyecto> --api-url http://127.0.0.1:18080
python ejecutar_lote.py --project-root <proyecto> --batch-id base --api-url http://127.0.0.1:18080 --db-url <DSN>
python verificar_lote.py --project-root <proyecto> --batch-id base
```

El cliente obtiene la sesión con `NIFI_USERNAME`/`NIFI_PASSWORD` o con las credenciales generadas del motor local indicado por `NIFI_RUNTIME`. No imprime ni exporta el token. El certificado local de laboratorio puede usar `NIFI_VERIFY_TLS=false`; fuera de localhost se exige `NIFI_ALLOW_INSECURE_HOST` explícito. En otro entorno usar TLS verificado.

El ejecutor dispara una sola página (máximo50000; es suficiente para los31065 eventos de este proyecto). Detiene las entradas, espera colas vacías y tareas finalizadas, reconcilia REST con landing/cuarentena, y escribe el manifiesto cerrado mediante reemplazo atómico. Si la fuente requiere paginación adicional o hay discrepancia, no sella el lote. Spark utiliza `data/manifests/base.json` y los JSONL de `data/landing/events/base`. El lote cerrado no se vuelve a disparar: se verifican sus hashes. Para continuar la demostración usar `--batch-id delta` después de publicar BASE.

El colector incluido registra cola, bytes, hilos, procesadores inválidos y boletines ERROR en PostgreSQL `ecommerce.system_metric`. Los boletines son recientes y pueden incluir un intento anterior ya corregido; el muestreo objetivo es15s, sujeto al tiempo de respuesta de NiFi. El tiempo de procesamiento medido no se presenta como un SLA de producción.

`probar_backpressure.py` crea un grupo aislado con cinco registros de prueba. Comprueba bloqueo estable y vaciado posterior; termina con ambos procesadores deshabilitados. Evidencia: `nifi_backpressure.json`. La primera comprobación del transporte detectó una propiedad antigua, comprobó cuatro intentos (original+3reintentos) y cuarentena; `nifi_preflight_http.json` conserva esa evidencia. La ejecución BASE usa la propiedad correcta `Socket Read Timeout`.

Los JSONL son objetos compactos separados por newline. Los importes permanecen en centavosBOB; `ingested_at`, `source` y `batch_id` se agregan después de la validación. El contador independiente reconcilia todas las ocurrencias (incluidos duplicados) y los objetos completos con la fuente inmutable.

La marca `ingested_at` se fija en UpdateAttribute «03 Registrar fuente y lote», después de SplitJson y antes de ValidateJson. La latencia `persisted_at − ingested_at` incluye validación, enriquecimiento, cierre de lotes y publicación de Spark; excluye descarga REST y espera de cola anterior a esa marca. BASE quedó cerrado con32JSONL/31045ocurrencias,100productos,20rechazosREST por esquema y1CSV rechazado. El tiempo total de ingesta medido entre inicio/cierre incluye consultas de control y drenado, por lo que difiere del tiempo de publicación Spark.
