📄 Proyecto Integrador: Pipeline ETLT de Clima a Data Lake en S3
1. Descripción del Proyecto
    Este proyecto implementa un pipeline ETLT (Extract, Load, Transform, Test) automatizado para procesar datos históricos de clima. Los datos crudos se ingieren en formato JSON en un Data Lake alojado en Amazon S3 y se transforman mediante PySpark, orquestado por Apache Airflow.

🌎 Arquitectura
La solución sigue una arquitectura basada en capas de Data Lake:

Capa	Ubicación	Formato	Propósito
RAW	s3a://pi-data-lake-alejandro/raw/weather_data/	JSON Anidado	Ingesta de datos tal como vienen de la fuente (Airbyte/API).
PROCESSED (Silver)	s3a://pi-data-lake-alejandro/processed/clima_historico/	Parquet	Datos limpios, aplanados, tipados y particionados, listos para análisis.

Exportar a Hojas de cálculo
2. Estructura del Repositorio
    El repositorio está organizado profesionalmente separando la lógica de la orquestación y la infraestructura:

    pi-data-engineering/
    ├── dags/
    │   └── etl_weather_dag.py          # DAG de Airflow (Orquestación - Avance #3 y #4)
    ├── scripts/
    │   └── transform_job.py            # Script PySpark (Transformación - Avance #2)
    ├── infra/
    │   └── docker-compose.yml          # Definición de servicios (Airflow, Postgres)
    ├── .github/
    │   └── workflows/
    │       └── main.yml                # Flujo de CI/CD (GitHub Actions - Avance #5)
    ├── README.md                       # Documentación del proyecto
    └── requirements.txt                # Dependencias de Python
    3. Requisitos Previos
    Para ejecutar el proyecto localmente (o en una máquina virtual/servidor):

    Docker y Docker Compose: Instalados y funcionales.

    Credenciales de AWS: El script PySpark requiere acceder al bucket S3. Configure las variables de entorno AWS en el host o en el contenedor de Airflow/Spark:

    Bash

    export AWS_ACCESS_KEY_ID=tu_access_key
    export AWS_SECRET_ACCESS_KEY=tu_secret_key
    Datos Crudos: Se debe subir al menos un archivo JSON con la estructura anidada de OpenWeatherMap a la carpeta s3a://pi-data-lake-alejandro/raw/weather_data/ antes de ejecutar el DAG.

4. Guía de Implementación y Ejecución
    Esta guía detalla la implementación desde cero del pipeline.

    Avance #1: Creación del Data Lake (AWS S3)
    Acción: Creación del bucket pi-data-lake-alejandro en la región AWS de su preferencia.

    Estructura: Creación de los prefijos (carpetas) iniciales: /raw/, /processed/, /gold/.

    Verificación: Subir un archivo JSON a la carpeta raw/weather_data/.

    Avance #2: Lógica de Transformación (PySpark)
    Archivo: scripts/transform_job.py

    Función: Este script se encarga de:

    Configurar PySpark con las correcciones necesarias para S3A (spark.hadoop.fs.s3a.threadpool.core.keepaliveTime = 60000, etc.).

    Leer los archivos JSON anidados desde la capa RAW de S3.

    Aplicar explode() sobre el array weather.

    Aplanar, limpiar y renombrar las columnas (ej. temp a temperatura_celsius).

    Escribir el resultado en formato Parquet en la capa PROCESSED, particionando por ciudad y fecha_analisis.

    Avance #3: Infraestructura y Orquestación
    Infraestructura (infra/docker-compose.yml): Se utiliza Docker Compose para levantar los servicios de Airflow (Webserver, Scheduler, Base de Datos).

    Orquestación (dags/etl_weather_dag.py):

    Define el DAG etl_json_weather_to_parquet.

    Utiliza el operador BashOperator para invocar el comando spark-submit dentro del contenedor de Airflow, cargando los paquetes de AWS necesarios.

    Avance #4: Despliegue y Ejecución del Pipeline (Comprobación)
    Levantar Airflow:

Bash

cd infraestructura/
docker compose up -d
Despliegue de Código: Asegúrese de que ambos archivos (etl_weather_dag.py y transform_job.py) estén en la carpeta local que Docker está leyendo como volumen de DAGs (generalmente la carpeta dags/ que acompaña al docker-compose.yml).

Acceso a la UI: Abra su navegador en http://localhost:8080.

Ejecución:

Busque el DAG etl_json_weather_to_parquet.

Active el interruptor (toggle).

Haga clic en "Trigger DAG" para disparar la ejecución.

Verificación de Logs: Acceda al Graph View del DAG, haga clic en la tarea ejecutar_transformacion_json_a_parquet, y revise los logs. La ejecución exitosa confirma que los datos fueron escritos en s3a://pi-data-lake-alejandro/processed/clima_historico/.

