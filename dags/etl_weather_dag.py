from datetime import datetime
import requests
import json
import os
import boto3 
import io 

from airflow import DAG
from airflow.operators.python import PythonOperator # pyright: ignore[reportMissingImports]
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.models import Variable 

# --------------------------------------------------------------------------
# CONFIGURACIÓN CRÍTICA
# --------------------------------------------------------------------------

WEATHER_API_KEY = '7e07d2adbc634aa58e234110250310'
WEATHER_API_URL = f'https://api.weatherapi.com/v1/current.json?q=Medellin&key={WEATHER_API_KEY}'

# Rutas y S3
S3_BUCKET_NAME = 'pi-data-lake-alejandro' 
S3_STAGING_KEY = 'raw/weather/raw_weather_data_staging.json'
S3_STAGING_PATH = f's3a://{S3_BUCKET_NAME}/{S3_STAGING_KEY}'

TRANSFORM_SCRIPT_PATH = '/opt/dags/transform_job.py' 
S3_OUTPUT_PATH = 's3a://pi-data-lake-alejandro/processed/clima_historico/'

AWS_ACCESS_KEY_ID = os.environ.get('AWS_ACCESS_KEY_ID', '') 
AWS_SECRET_ACCESS_KEY = os.environ.get('AWS_SECRET_ACCESS_KEY', '')

default_args = {
    'owner': 'Alejandro Arango',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}

# --------------------------------------------------------------------------
# DEFINICIÓN DE TAREA PYTHON: Extracción y Carga a S3
# --------------------------------------------------------------------------
def fetch_and_upload_weather_data(**kwargs):
    # ... (El cuerpo de esta función se mantiene sin cambios, asumiendo que boto3 está instalado) ...
    print(f"Iniciando extracción de datos desde: {WEATHER_API_URL}")
    
    try:
        response = requests.get(WEATHER_API_URL)
        response.raise_for_status() 
        data = response.json()
        
        json_data = json.dumps(data, indent=4)
        print("Datos extraídos exitosamente.")

        s3 = boto3.client('s3', 
                          aws_access_key_id=AWS_ACCESS_KEY_ID, 
                          aws_secret_access_key=AWS_SECRET_ACCESS_KEY)
        
        s3.put_object(Bucket=S3_BUCKET_NAME, Key=S3_STAGING_KEY, Body=json_data)

        print(f"Datos cargados a S3 exitosamente en: {S3_STAGING_PATH}")
        
        return S3_STAGING_PATH
        
    except requests.exceptions.RequestException as e:
        print(f"Error al conectar con WeatherAPI o recibir respuesta: {e}")
        raise

with DAG(
    dag_id='etl_json_weather_to_parquet',
    default_args=default_args,
    schedule_interval='@daily', # pyright: ignore[reportCallIssue]
    catchup=False,
    tags=['Datatake', 'PySpark', 'DirectFetch', 'S3'],
) as dag:

    fetch_data_task = PythonOperator(
        task_id='extraer_y_guardar_json_clima',
        python_callable=fetch_and_upload_weather_data,
        provide_context=True,
    )

    pyspark_transform_task = SparkSubmitOperator(
        task_id='ejecutar_transformacion_json_a_parquet',
        
        application=TRANSFORM_SCRIPT_PATH, 
        conn_id='spark_default',
        
        application_args=[
            S3_STAGING_PATH,
            S3_OUTPUT_PATH
        ],
        conf={
            "spark.master": "local[*]",
            "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            "spark.hadoop.fs.s3a.connection.timeout": "50000",
            "spark.hadoop.fs.s3a.fast.upload": "true",
            "spark.hadoop.fs.s3a.aws.credentials.provider": "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider", 
            "spark.hadoop.fs.s3a.access.key": AWS_ACCESS_KEY_ID, 
            "spark.hadoop.fs.s3a.secret.key": AWS_SECRET_ACCESS_KEY,
        },
        
        packages="org.apache.hadoop:hadoop-aws:3.3.2,com.amazonaws:aws-java-sdk-bundle:1.12.308", 
        name='arrow-spark',
        deploy_mode='client',
    )
    
    fetch_data_task >> pyspark_transform_task # pyright: ignore[reportUnusedExpression]