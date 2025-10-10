import sys
import logging
import os
import platform
import traceback 
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, lit, round
# 🔑 ASEGÚRATE DE QUE ESTA LÍNEA ESTÉ PRESENTE Y CORRECTA
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType 

# ---------------------------------------------------------------------------------
# 1. CONFIGURACIÓN Y DEFINICIÓN EXPLÍCITA DEL ESQUEMA
# ---------------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Definición del esquema para 'condition'
condition_schema = StructType([
    StructField("text", StringType(), True)
])

# Definición del esquema para 'current'
current_schema = StructType([
    StructField("last_updated", StringType(), True),
    StructField("temp_c", DoubleType(), True),
    StructField("humidity", IntegerType(), True),
    StructField("pressure_mb", DoubleType(), True),
    StructField("condition", condition_schema, True), 
])

# Definición del esquema principal (Weather API)
WEATHER_SCHEMA = StructType([
    StructField("location", StructType([
        StructField("name", StringType(), True),
        StructField("region", StringType(), True),
        StructField("country", StringType(), True),
        StructField("lat", DoubleType(), True),
        StructField("lon", DoubleType(), True)
    ]), True),
    StructField("current", current_schema, True) 
])


def main():
    """
    Función principal que ejecuta el proceso ETL.
    """
    
    # ---------------------------------------------------------------------------------
    # 2. CAPTURA DE ARGUMENTOS DE ENTRADA/SALIDA
    # ---------------------------------------------------------------------------------
    if len(sys.argv) != 3:
        logger.error("❌ Uso: spark-submit transform_job.py <ruta_s3_entrada> <ruta_parquet_salida>")
        sys.exit(1)

    JSON_S3_INPUT_PATH = sys.argv[1] 
    PROCESSED_PATH = sys.argv[2]      

    logger.info(f"Ruta JSON de entrada (S3): {JSON_S3_INPUT_PATH}")
    logger.info(f"Ruta de salida Parquet (S3): {PROCESSED_PATH}")
    
    # ---------------------------------------------------------------------------------
    # 3. INICIAR SPARK SESSION Y EJECUTAR ETL DENTRO DE UN BLOQUE TRY/EXCEPT
    # ---------------------------------------------------------------------------------
    spark = None
    
    try:
        logger.info("Iniciando Spark Session...")
        spark = SparkSession.builder.appName("PI_Transformacion_Clima_Directo").getOrCreate()
        
        # ---------------------------------------------------------------------------------
        # 4. LEER DATOS DESDE RUTA S3 (USANDO ESQUEMA FORZADO)
        # ---------------------------------------------------------------------------------
        logger.info(f"Intentando leer datos desde S3 usando el esquema forzado: {JSON_S3_INPUT_PATH}")
        
        df_raw = spark.read \
            .option("multiline", "true") \
            .schema(WEATHER_SCHEMA) \
            .json(JSON_S3_INPUT_PATH)
        
        logger.info("✅ Lectura de S3 exitosa. Esquema del DataFrame crudo:")
        df_raw.printSchema()

        # Diagnóstico de conteo de filas
        record_count = df_raw.count()
        if record_count == 0: # pyright: ignore[reportOperatorIssue]
            logger.error("❌ Error de datos: El DataFrame resultante está vacío (0 filas). El JSON está vacío o malformado.")
            sys.exit(1)
        logger.info(f"Se cargaron {record_count} registros.")
        
        # ---------------------------------------------------------------------------------
        # 5. TRANSFORMACIÓN DE DATOS
        # ---------------------------------------------------------------------------------
        logger.info("Iniciando transformación de datos...")
        df_transformado = df_raw.select(
            # Extracción de campos de 'location'
            col("location.name").alias("ciudad"),
            col("location.region").alias("region"),
            col("location.country").alias("pais"),
            col("location.lat").alias("latitud"),
            col("location.lon").alias("longitud"),
            
            # Extracción y transformación de campos de 'current'
            col("current.last_updated").cast("timestamp").alias("fecha_hora_medicion"),
            to_date(col("current.last_updated")).alias("fecha_analisis"),
            round(col("current.temp_c").cast("double"), 2).alias("temperatura_celsius"),
            col("current.humidity").alias("humedad_porcentaje"),
            col("current.pressure_mb").alias("presion_milibares"),
            col("current.condition.text").alias("descripcion_clima"), 
            
            # Campos de metadatos del pipeline
            current_timestamp().alias("fecha_procesamiento_pipeline")
        ).withColumn("fuente_dato", lit("WeatherAPI_DirectFetch"))
        
        # ---------------------------------------------------------------------------------
        # 6. ESCRIBIR RESULTADOS EN S3
        # ---------------------------------------------------------------------------------
        logger.info(f"Escribiendo {record_count} registros procesados en: {PROCESSED_PATH}")
        df_transformado.write \
            .mode("overwrite") \
            .partitionBy("ciudad", "fecha_analisis") \
            .option("compression", "snappy") \
            .parquet(PROCESSED_PATH)

        logger.info("✅ ¡Transformación PySpark completada! Datos listos en formato Parquet.")

    except Exception as e:
        # 🛑 CAPTURA DEL ERROR DETALLADO
        logger.error("**************************************************")
        logger.error("❌ FALLO DE ETL EN PYSPARK: Se capturó la siguiente excepción:")
        logger.error(f"Mensaje: {e}")
        logger.error(traceback.format_exc()) 
        logger.error("**************************************************")
        sys.exit(1) 
        
    finally:
        if spark:
            spark.stop()

# Punto de entrada del script
if __name__ == "__main__":
    main()