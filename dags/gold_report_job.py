import logging
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, lit, expr, rank, row_number
from pyspark.sql.window import Window

# --- Logging Setup (Inferred from log) ---
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# --- Configuration (Rutas de datos actualizadas con el bucket real) ---
# Se utiliza el bucket proporcionado por el usuario (pi-data-lake-alejandro)
# Se asume una estructura estándar de data lake (bronze/ -> gold/)
BRONZE_DATA_PATH = "s3a://pi-data-lake-alejandro/bronze/weather_data" 
# ACTUALIZACIÓN: La ruta de GOLD ahora apunta a una carpeta para CSV
GOLD_DATA_PATH = "s3a://pi-data-lake-alejandro/gold/best_days_report_csv" 

import logging
import sys
import traceback
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc, asc, row_number, when, min, max
from pyspark.sql.window import Window

# --- Logging Setup ---
logging.basicConfig(level=logging.INFO, stream=sys.stdout)
logger = logging.getLogger(__name__)

# --- Configuration (Rutas de datos actualizadas con el bucket real) ---
BRONZE_DATA_PATH = "s3a://pi-data-lake-alejandro/bronze/weather_data" 
S3_BUCKET = "s3a://pi-data-lake-alejandro/gold/"

# Rutas para los tres nuevos reportes
REPORT_1_BEST_WORST_PATH = S3_BUCKET + "best_and_worst_days_report"
REPORT_2_CLIMATE_POTENTIAL_PATH = S3_BUCKET + "climate_potential_report"
REPORT_3_PREDICTION_COMPARISON_PATH = S3_BUCKET + "prediction_comparison_report"

def read_and_prepare_data(spark):
    """Reads the data and calculates the daily average energy potential."""
    logger.info(f"Attempting to read data from: {BRONZE_DATA_PATH}")
    
    # --- Mock Data Generation (Replace with spark.read.parquet(BRONZE_DATA_PATH) in production) ---
    data = [
        ("New York", "NY", "USA", 40.71, -74.00, "2025-10-23 10:00:00", "2025-10-23", 20.0, 50.0, 1012, "Clear", "2025-10-23", "API"),
        ("New York", "NY", "USA", 40.71, -74.00, "2025-10-23 11:00:00", "2025-10-23", 22.0, 40.0, 1011, "Clear", "2025-10-23", "API"),
        # Día con bajo potencial (lluvia/nieve y humedad alta)
        ("New York", "NY", "USA", 40.71, -74.00, "2025-10-24 10:00:00", "2025-10-24", 10.0, 95.0, 1005, "Heavy Rain", "2025-10-24", "API"), 
        ("New York", "NY", "USA", 40.71, -74.00, "2025-10-25 10:00:00", "2025-10-25", 25.0, 30.0, 1010, "Sunny", "2025-10-25", "API"),

        ("Los Angeles", "CA", "USA", 34.05, -118.24, "2025-10-23 10:00:00", "2025-10-23", 28.0, 30.0, 1015, "Sunny", "2025-10-23", "API"),
        ("Los Angeles", "CA", "USA", 34.05, -118.24, "2025-10-24 10:00:00", "2025-10-24", 26.0, 35.0, 1014, "Sunny", "2025-10-24", "API"),
        # Ejemplo de datos de predicción (para el reporte 3)
        ("Los Angeles", "CA", "USA", 34.05, -118.24, "2025-10-24 10:00:00", "2025-10-24", 27.0, 30.0, 1015, "Sunny", "2025-10-24", "Model"),
    ]
    columns = [
        "ciudad", "region", "pais", "latitud", "longitud", "fecha_hora_medicion",
        "fecha_analisis", "temperatura_celsius", "humedad_porcentaje", "presion_milibares",
        "descripcion_clima", "fecha_procesamiento_pipeline", "fuente_dato"
    ]
    df_read = spark.createDataFrame(data, columns)
    # df_read = spark.read.parquet(BRONZE_DATA_PATH)
    
    # 1. Calculate 'potencial_energetico' (Energy Potential)
    df_with_pe = df_read.withColumn(
        "potencial_energetico",
        (col("temperatura_celsius") - (col("humedad_porcentaje").cast("double") * 0.1))
    )
    
    # 2. Aggregate to find the daily average potential
    df_ranking = df_with_pe.groupBy("ciudad", "pais", "fecha_analisis", "descripcion_clima").agg(
        avg("potencial_energetico").alias("promedio_potencial")
    ).sort(desc("promedio_potencial"))

    return df_ranking, df_with_pe # Retorna la data agregada y la data por medición

def write_csv_report(df, path, report_name):
    """Writes a DataFrame to S3 as a CSV file with header."""
    logger.info(f"Writing {report_name} report to: {path}")
    df.write \
        .mode("overwrite") \
        .option("header", "true") \
        .csv(path)
    logger.info(f"✅ {report_name} generado exitosamente en formato CSV y guardado en S3.")

def main():
    """Main execution function."""
    spark = None
    try:
        spark = SparkSession.builder.appName("spark-gold-report").getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")

        df_ranking, df_with_pe = read_and_prepare_data(spark)
        
        # ----------------------------------------------------------------------
        # REPORTE 1: Días con Mayor y Menor Potencial (Responde pregunta 3)
        # ----------------------------------------------------------------------
        logger.info("Generating Report 1: Best and Worst Days per location.")
        window_spec = Window.partitionBy("ciudad", "pais").orderBy(desc("promedio_potencial"))
        
        # Usamos dos windows para rankear del mejor al peor
        df_ranked = df_ranking.withColumn(
            "rank_best", row_number().over(window_spec) # 1 = Best Day
        ).withColumn(
            "rank_worst", row_number().over(Window.partitionBy("ciudad", "pais").orderBy(asc("promedio_potencial"))) # 1 = Worst Day
        )
        
        # Filtramos solo los mejores (rank_best=1) y los peores (rank_worst=1)
        df_best_worst = df_ranked.filter((col("rank_best") == 1) | (col("rank_worst") == 1)) \
                                 .withColumn("tipo_dia", when(col("rank_best") == 1, "MAXIMO_POTENCIAL").otherwise("MINIMO_POTENCIAL")) \
                                 .select("ciudad", "pais", "fecha_analisis", "promedio_potencial", "tipo_dia", "descripcion_clima")
        
        df_best_worst.show(truncate=False)
        write_csv_report(df_best_worst, REPORT_1_BEST_WORST_PATH, "Best and Worst Days")
        
        # ----------------------------------------------------------------------
        # REPORTE 2: Clima Asociado a Reducciones (Responde pregunta 1)
        # ----------------------------------------------------------------------
        logger.info("Generating Report 2: Climate Conditions causing low potential.")
        
        # Agregamos por tipo de clima para ver el potencial promedio de ese clima
        df_climate_impact = df_ranking.groupBy("descripcion_clima", "pais").agg(
            avg("promedio_potencial").alias("potencial_promedio_por_clima"),
            min("promedio_potencial").alias("min_potencial_observado"),
            max("promedio_potencial").alias("max_potencial_observado"),
            col("pais").alias("pais_clima")
        ).select("descripcion_clima", "pais_clima", "potencial_promedio_por_clima", "min_potencial_observado")
        
        df_climate_impact.show(truncate=False)
        write_csv_report(df_climate_impact, REPORT_2_CLIMATE_POTENTIAL_PATH, "Climate Potential Impact")

        # ----------------------------------------------------------------------
        # REPORTE 3: Comparación de Predicciones (Responde pregunta 2)
        # ----------------------------------------------------------------------
        logger.info("Generating Report 3: Observation vs. Prediction Comparison.")
        
        # Separamos las mediciones por fuente (API vs. Model) y calculamos el promedio diario
        df_comparison = df_with_pe.groupBy("ciudad", "pais", "fecha_analisis", "fuente_dato").agg(
            avg("potencial_energetico").alias("promedio_potencial")
        )

        # Pivoteamos la tabla para tener las columnas de Observado y Predicción lado a lado
        df_pivot = df_comparison.groupBy("ciudad", "pais", "fecha_analisis").pivot("fuente_dato").agg(
            avg("promedio_potencial")
        )

        # Calculamos la diferencia
        if "API" in df_pivot.columns and "Model" in df_pivot.columns:
            df_final_comparison = df_pivot.withColumn(
                "diferencia_observado_vs_prediccion",
                col("API") - col("Model")
            ).withColumnRenamed("API", "Potencial_Observado").withColumnRenamed("Model", "Potencial_Predicho")
        else:
            # Manejo de caso donde solo tenemos una fuente (como en el Mock actual)
            logger.warn("Only one data source (API or Model) found. Cannot calculate difference.")
            df_final_comparison = df_pivot 

        df_final_comparison.show(truncate=False)
        write_csv_report(df_final_comparison, REPORT_3_PREDICTION_COMPARISON_PATH, "Prediction Comparison")
        
    except Exception as e:
        logger.error("❌ FALLO AL GENERAR REPORTE GOLD:")
        logger.error(f"Mensaje: {e};")
        logger.error(traceback.format_exc()) 
        if spark:
            spark.stop()
        sys.exit(1)
        
    finally:
        if spark:
            spark.stop()

if __name__ == "__main__":
    main()
