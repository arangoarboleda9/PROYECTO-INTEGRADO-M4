from datetime import datetime
import requests
import json
import os

from airflow import DAG
from airflow.operators.python import PythonOperator  # pyright: ignore[reportMissingImports]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable 

# --------------------------------------------------------------------------
# CONFIGURACIÓN CRÍTICA
# --------------------------------------------------------------------------

# Clave de WeatherAPI para la extracción directa
WEATHER_API_KEY = '7e07d2adbc634aa58e234110250310'
WEATHER_API_URL = f'https://api.weatherapi.com/v1/current.json?q=Medellin&key={WEATHER_API_KEY}'

# Rutas de archivos y scripts
JSON_INPUT_PATH = '/tmp/raw_weather_data.json'

# RUTA CRÍTICA DENTRO DEL CONTENEDOR DE AIRFLOW:
# Esta ruta absoluta es usada por el parámetro 'files' para COPIAR el script al driver de Spark.
TRANSFORM_SCRIPT_PATH = '/opt/dags/transform_job.py' 

# IMPORTANTE: Obtener credenciales de AWS del entorno (como fallback)
AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', 'TU_ACCESS_KEY_AQUI') 
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', 'TU_SECRET_KEY_AQUI')

# Ruta S3 para la salida (debe terminar en /)
S3_OUTPUT_PATH = 's3a://pi-data-lake-alejandro/processed/clima_actual/'


default_args = {
    'owner': 'Alejandro Arango',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# --------------------------------------------------------------------------
# DEFINICIÓN DE TAREA PYTHON: Extracción Directa
# --------------------------------------------------------------------------
def fetch_and_save_weather_data(**kwargs):
    """
    Extrae datos de WeatherAPI y guarda el JSON en un archivo local.
    """
    print(f"Iniciando extracción de datos desde: {WEATHER_API_URL}")
    
    try:
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status() 
        data = response.json()
        print(f"Guardando datos en {JSON_INPUT_PATH}...")
        
        # Garantizar que el directorio /tmp existe 
        os.makedirs(os.path.dirname(JSON_INPUT_PATH), exist_ok=True)
        
        with open(JSON_INPUT_PATH, 'w') as f:
            json.dump(data, f, indent=4) 
        
        print(f"Datos guardados exitosamente en {JSON_INPUT_PATH}")
        
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con WeatherAPI o recibir respuesta: {e}")
        raise

with DAG(
    dag_id='etl_json_weather_to_parquet',
    default_args=default_args,
    schedule_interval='@daily', # pyright: ignore[reportCallIssue]
    catchup=False,
    tags=['Datatake', 'PySpark', 'DirectFetch', 'Weather'],
) as dag:

    # ----------------------------------------------------------------------
    # TAREA 1: Extracción Directa de JSON
    # ----------------------------------------------------------------------
    fetch_data_task = PythonOperator(
        task_id='extraer_y_guardar_json_clima',
        python_callable=fetch_and_save_weather_data,
        provide_context=True,
    )

    # ----------------------------------------------------------------------
    # TAREA 2: Transformación PySpark
    # ----------------------------------------------------------------------
    pyspark_transform_task = SparkSubmitOperator(
        task_id='ejecutar_transformacion_json_a_parquet',
        
        # 1. Nombre base del archivo a EJECUTAR (Spark lo encuentra en su CWD después de la copia)
        application=TRANSFORM_SCRIPT_PATH, 
        conn_id='spark_default',
        
        # 2. Ruta absoluta del archivo a COPIAR (Airflow lo copia de aquí a Spark)
        #files=TRANSFORM_SCRIPT_PATH, 

        # Argumentos para tu script transform_job.py: [Ruta JSON de entrada, Ruta S3 de salida]
        application_args=[JSON_INPUT_PATH, S3_OUTPUT_PATH], 
        
        conf={
            "spark.master": "local[*]",
            # Configuración S3A: Implementación y proveedor
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", 
            
            # INYECCIÓN DE CREDENCIALES: S3A Credentials
            "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID, 
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
        },
        
        # Paquetes Jars para conectividad S3A
        packages='org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-s3:1.12.308',
        name='arrow-spark',
        deploy_mode='client',
    )
    
    # ----------------------------------------------------------------------
    # Cadena de Tareas
    # ----------------------------------------------------------------------
    fetch_data_task >> pyspark_transform_task # pyright: ignore[reportUnusedExpression]
