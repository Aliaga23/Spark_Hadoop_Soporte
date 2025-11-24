from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import os

spark = SparkSession.builder \
    .appName("Crear Zonas por Densidad") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

print("=" * 80)
print("CREANDO ZONAS GEOGRAFICAS POR DENSIDAD")
print("=" * 80)

df = spark.read.csv("locations_rows.csv", header=True, inferSchema=True)

df = df.filter(
    col("latitude").isNotNull() & 
    col("longitude").isNotNull()
)

print(f"\nTotal puntos GPS: {df.count():,}")

grid_size = 0.04
df = df.withColumn("grid_lat", (col("latitude") / grid_size).cast("integer"))
df = df.withColumn("grid_lon", (col("longitude") / grid_size).cast("integer"))

zonas = df.groupBy("grid_lat", "grid_lon") \
    .agg(
        count("*").alias("total_mediciones"),
        round(avg("latitude"), 6).alias("centro_lat"),
        round(avg("longitude"), 6).alias("centro_lon"),
        round(avg("altitude"), 2).alias("altitud_promedio")
    ) \
    .orderBy(col("total_mediciones").desc())

print(f"\nZonas con >= 100 mediciones: {zonas.count()}")

zonas = zonas.withColumn("zona_id", monotonically_increasing_id() + 1)

zonas = zonas.withColumn("zona_nombre",
    concat(
        lit("ZONA_"),
        col("zona_id"),
        lit("_"),
        when(col("total_mediciones") >= 2000, "ALTA")
        .when(col("total_mediciones") >= 500, "MEDIA")
        .otherwise("BAJA")
    )
)

zonas = zonas.withColumn("grid_latitud_inicio", col("grid_lat") * grid_size)
zonas = zonas.withColumn("grid_latitud_fin", (col("grid_lat") + 1) * grid_size)
zonas = zonas.withColumn("grid_longitud_inicio", col("grid_lon") * grid_size)
zonas = zonas.withColumn("grid_longitud_fin", (col("grid_lon") + 1) * grid_size)

zonas_final = zonas.select(
    "zona_id",
    "zona_nombre",
    "centro_lat",
    "centro_lon",
    "total_mediciones",
    "altitud_promedio",
    "grid_latitud_inicio",
    "grid_latitud_fin",
    "grid_longitud_inicio",
    "grid_longitud_fin"
).orderBy("zona_id")

print("\nZonas creadas (Top 10):")
zonas_final.show(10, truncate=False)

output_dir = "output/datawarehouse"
os.makedirs(output_dir, exist_ok=True)

zonas_pandas = zonas_final.toPandas()
zonas_pandas.to_csv(f"{output_dir}/DIM_ZONAS.csv", index=False)

print(f"\n✓ DIM_ZONAS.csv - {len(zonas_pandas):,} zonas creadas")

print("\nEstadísticas:")
print(f"  Zona con más mediciones: {zonas_pandas['total_mediciones'].max():,}")
print(f"  Zona con menos mediciones: {zonas_pandas['total_mediciones'].min():,}")
print(f"  Promedio por zona: {zonas_pandas['total_mediciones'].mean():.0f}")

spark.stop()
