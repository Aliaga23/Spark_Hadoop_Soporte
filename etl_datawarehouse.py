from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from pyspark.sql.types import *
import os

spark = SparkSession.builder \
    .appName("DataWarehouse ETL") \
    .config("spark.driver.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

print("=" * 80)
print("ETL DATA WAREHOUSE - COBERTURA MOVIL")
print("=" * 80)

print("\n[1/9] Cargando datos origen...")
df = spark.read.csv("locations_rows.csv", header=True, inferSchema=True)
print(f"   Total registros: {df.count():,}")

print("\n[2/9] Limpiando y preparando datos...")
df = df.filter(
    col("latitude").isNotNull() & 
    col("longitude").isNotNull() & 
    col("timestamp").isNotNull()
)

df = df.withColumn("timestamp", to_timestamp(col("timestamp")))

df = df.withColumn("operador_normalizado", 
    when(upper(col("sim_operator")).contains("ENTEL"), "ENTEL")
    .when(upper(col("sim_operator")).contains("TIGO"), "TIGO")
    .when(upper(col("sim_operator")).contains("VIVA"), "VIVA")
    .when(upper(col("sim_operator")).contains("MOVIL"), "MOVIL")
    .when(upper(col("sim_operator")).contains("BOMOV"), "BOMOV")
    .otherwise(upper(col("sim_operator")))
)

df = df.withColumn("red_normalizada",
    when(upper(col("network_type")).contains("LTE"), "LTE")
    .when(upper(col("network_type")).contains("4G"), "4G")
    .when(upper(col("network_type")).contains("3G"), "3G")
    .when(upper(col("network_type")).contains("WIFI"), "WIFI")
    .when(upper(col("network_type")).contains("MOBILE"), "MOBILE")
    .otherwise(upper(col("network_type")))
)

df = df.withColumn("calidad_senal",
    when(col("signal") >= -70, "EXCELENTE")
    .when(col("signal") >= -85, "BUENA")
    .when(col("signal") >= -95, "REGULAR")
    .when(col("signal") >= -105, "MALA")
    .otherwise("CRITICA")
)

df = df.withColumn("categoria_velocidad",
    when(col("speed") < 1, "ESTATICO")
    .when(col("speed") < 7, "CAMINANDO")
    .when(col("speed") < 20, "BICICLETA")
    .when(col("speed") < 60, "VEHICULO")
    .otherwise("ALTA_VELOCIDAD")
)

df = df.withColumn("zona_altitud",
    when(col("altitude") < 2500, "BAJA")
    .when(col("altitude") < 3000, "MEDIA")
    .otherwise("ALTA")
)

df = df.withColumn("lat_sector", (col("latitude") * 10000).cast("integer"))
df = df.withColumn("lon_sector", (col("longitude") * 10000).cast("integer"))
df = df.withColumn("sector_id", concat(col("lat_sector"), lit("_"), col("lon_sector")))

print(f"   Registros válidos: {df.count():,}")

print("\n[3/9] Creando DIM_TIEMPO...")
dim_tiempo = df.select("timestamp").distinct() \
    .withColumn("fecha", to_date(col("timestamp"))) \
    .withColumn("anio", year(col("fecha"))) \
    .withColumn("trimestre", quarter(col("fecha"))) \
    .withColumn("mes", month(col("fecha"))) \
    .withColumn("semana_anio", weekofyear(col("fecha"))) \
    .withColumn("dia_mes", dayofmonth(col("fecha"))) \
    .withColumn("dia_semana", dayofweek(col("fecha"))) \
    .withColumn("nombre_dia", 
        when(col("dia_semana") == 1, "DOMINGO")
        .when(col("dia_semana") == 2, "LUNES")
        .when(col("dia_semana") == 3, "MARTES")
        .when(col("dia_semana") == 4, "MIERCOLES")
        .when(col("dia_semana") == 5, "JUEVES")
        .when(col("dia_semana") == 6, "VIERNES")
        .otherwise("SABADO")
    ) \
    .withColumn("es_fin_semana", 
        when(col("dia_semana").isin([1, 7]), "SI").otherwise("NO")
    ) \
    .select("fecha", "anio", "trimestre", "mes", "semana_anio", 
            "dia_mes", "dia_semana", "nombre_dia", "es_fin_semana") \
    .distinct() \
    .orderBy("fecha")

dim_tiempo = dim_tiempo.withColumn("tiempo_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_tiempo.count()}")

print("\n[4/9] Creando DIM_HORA...")
dim_hora = df.select("timestamp").distinct() \
    .withColumn("hora", hour(col("timestamp"))) \
    .withColumn("franja_horaria",
        when(col("hora").between(6, 11), "MAÑANA")
        .when(col("hora").between(12, 18), "TARDE")
        .when(col("hora").between(19, 23), "NOCHE")
        .otherwise("MADRUGADA")
    ) \
    .select("hora", "franja_horaria") \
    .distinct() \
    .orderBy("hora")

dim_hora = dim_hora.withColumn("hora_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_hora.count()}")

print("\n[5/9] Creando DIM_OPERADOR...")
dim_operador = df.select("operador_normalizado").distinct() \
    .filter(col("operador_normalizado").isNotNull()) \
    .orderBy("operador_normalizado")

dim_operador = dim_operador.withColumn("operador_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_operador.count()}")

print("\n[6/9] Creando DIM_RED...")
dim_red = df.select("red_normalizada").distinct() \
    .filter(col("red_normalizada").isNotNull()) \
    .orderBy("red_normalizada")

dim_red = dim_red.withColumn("red_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_red.count()}")

print("\n[7/9] Creando DIM_CALIDAD...")
dim_calidad = df.select("calidad_senal").distinct() \
    .orderBy(
        when(col("calidad_senal") == "EXCELENTE", 1)
        .when(col("calidad_senal") == "BUENA", 2)
        .when(col("calidad_senal") == "REGULAR", 3)
        .when(col("calidad_senal") == "MALA", 4)
        .otherwise(5)
    )

dim_calidad = dim_calidad.withColumn("calidad_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_calidad.count()}")

print("\n[8/9] Creando DIM_VELOCIDAD...")
dim_velocidad = df.groupBy("categoria_velocidad") \
    .agg(round(avg("speed"), 2).alias("velocidad_promedio")) \
    .orderBy(
        when(col("categoria_velocidad") == "ESTATICO", 1)
        .when(col("categoria_velocidad") == "CAMINANDO", 2)
        .when(col("categoria_velocidad") == "BICICLETA", 3)
        .when(col("categoria_velocidad") == "VEHICULO", 4)
        .otherwise(5)
    )

dim_velocidad = dim_velocidad.withColumn("velocidad_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_velocidad.count()}")

print("\n[9/9] Creando DIM_UBICACION...")
dim_ubicacion = df.groupBy("sector_id", "zona_altitud") \
    .agg(
        round(avg("latitude"), 6).alias("centro_lat"),
        round(avg("longitude"), 6).alias("centro_lon"),
        round(avg("altitude"), 2).alias("altitud_promedio"),
        round(min("altitude"), 2).alias("altitud_minima"),
        round(max("altitude"), 2).alias("altitud_maxima")
    ) \
    .orderBy("sector_id")

dim_ubicacion = dim_ubicacion.withColumn("ubicacion_id", monotonically_increasing_id() + 1)
print(f"   Registros (sectores): {dim_ubicacion.count()}")

print("\n[10/9] Creando DIM_DISPOSITIVO...")
dim_dispositivo = df.select("device_name").distinct() \
    .filter(col("device_name").isNotNull()) \
    .orderBy("device_name")

dim_dispositivo = dim_dispositivo.withColumn("dispositivo_id", monotonically_increasing_id() + 1)
print(f"   Registros: {dim_dispositivo.count()}")

print("\n[11/11] Creando FACT_MEDICIONES...")

df = df.withColumn("fecha", to_date(col("timestamp"))) \
       .withColumn("hora", hour(col("timestamp")))

fact = df \
    .join(dim_tiempo.select("tiempo_id", "fecha"), "fecha", "left") \
    .join(dim_hora.select("hora_id", "hora"), "hora", "left") \
    .join(dim_operador.select("operador_id", "operador_normalizado"), "operador_normalizado", "left") \
    .join(dim_red.select("red_id", "red_normalizada"), "red_normalizada", "left") \
    .join(dim_calidad.select("calidad_id", "calidad_senal"), "calidad_senal", "left") \
    .join(dim_velocidad.select("velocidad_id", "categoria_velocidad"), "categoria_velocidad", "left") \
    .join(dim_ubicacion.select("ubicacion_id", "sector_id"), "sector_id", "left") \
    .join(dim_dispositivo.select("dispositivo_id", "device_name"), "device_name", "left")

fact_mediciones = fact.select(
    monotonically_increasing_id().alias("medicion_id"),
    col("timestamp"),
    col("tiempo_id"),
    col("hora_id"),
    col("ubicacion_id"),
    col("operador_id"),
    col("red_id"),
    col("calidad_id"),
    col("velocidad_id"),
    col("dispositivo_id"),
    col("signal").alias("medida_senal"),
    col("speed").alias("medida_velocidad"),
    col("altitude").alias("medida_altitud"),
    col("latitude"),
    col("longitude")
).orderBy("timestamp", "dispositivo_id")

print(f"   Registros: {fact_mediciones.count():,}")

print("\n" + "=" * 80)
print("EXPORTANDO TABLAS A CSV...")
print("=" * 80)

output_dir = "output/datawarehouse"
os.makedirs(output_dir, exist_ok=True)

def exportar_csv(df, nombre):
    print(f"\nExportando {nombre}...")
    pandas_df = df.toPandas()
    pandas_df.to_csv(f"{output_dir}/{nombre}.csv", index=False)
    print(f"   ✓ {nombre}.csv - {len(pandas_df):,} registros")

exportar_csv(dim_tiempo, "DIM_TIEMPO")
exportar_csv(dim_hora, "DIM_HORA")
exportar_csv(dim_operador, "DIM_OPERADOR")
exportar_csv(dim_red, "DIM_RED")
exportar_csv(dim_calidad, "DIM_CALIDAD")
exportar_csv(dim_velocidad, "DIM_VELOCIDAD")
exportar_csv(dim_ubicacion, "DIM_UBICACION")
exportar_csv(dim_dispositivo, "DIM_DISPOSITIVO")
exportar_csv(fact_mediciones, "FACT_MEDICIONES")


spark.stop()
