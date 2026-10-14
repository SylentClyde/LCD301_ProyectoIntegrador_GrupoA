# Dependencias y alcance de distribución

Python 3.12, Java 21, Apache NiFi 2.12.0, Apache Spark 4.0.1, PostgreSQL 17.6 y driver JDBC PostgreSQL 42.7.7. Las bibliotecas Python se fijan en `requirements.txt`. Spark se distribuye como dependencia PySpark; su ejecución sigue siendo Apache Spark real.

Los motores, el entorno Python, los binarios de Java y los directorios operativos de NiFi no se incluyen en la entrega: son software de terceros, voluminoso, y pueden contener credenciales o datos de otros trabajos. El ZIP incluye el código, el flujo exportado, los datos sintéticos, los resultados y el respaldo del proyecto. No incluye credenciales de servicios personales.

Instalar desde las distribuciones oficiales y respetar sus licencias. NiFi y Spark: Apache License 2.0; PostgreSQL y su JDBC: licencias PostgreSQL; Dash/Plotly y FastAPI: MIT. El archivo de dependencias no sustituye las licencias completas de las distribuciones.

Referencias oficiales:

- [NiFi: descargas y documentación](https://nifi.apache.org/download/)
- [Spark 4.0.1](https://spark.apache.org/docs/4.0.1/)
- [PostgreSQL: descargas Windows](https://www.postgresql.org/download/windows/)
- [PostgreSQL JDBC](https://jdbc.postgresql.org/download/)
- [Java Temurin](https://adoptium.net/temurin/releases/?version=21)

El despliegue Docker tiene su propio README y un estado de verificación explícito. La evidencia de ejecución entregada corresponde al laboratorio nativo Windows.
