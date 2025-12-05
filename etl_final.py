"""
ETL FINAL OPTIMIZADO - Sistema de Tracking
===========================================
Basado en etl_datawarehouse.py con batch inserts ultra-rápidos.
Usa execute_values para inserción masiva (10-50x más rápido).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from supabase import create_client, Client
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from datetime import datetime
import sys

# ============================================================================
# CONFIGURACIÓN
# ============================================================================

pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

SUPABASE_URL = "https://lmqpbtuljodwklxdixjq.supabase.co"
SUPABASE_KEY = "sb_publishable_JeXh7gEgHiVx1LQBCcFidA_Ki0ARx4F"
GRID_SIZE = 0.02

# ============================================================================
# SPARK
# ============================================================================

spark = SparkSession.builder \
    .appName("ETL Final Optimizado") \
    .config("spark.driver.memory", "8g") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ============================================================================
# FUNCIONES DB OPTIMIZADAS
# ============================================================================

def get_conn():
    return psycopg2.connect(**pg_config)

def batch_insert(table, data, columns, conflict_col=None):
    """Insert masivo con execute_values (ultra rápido)"""
    if not data:
        return 0
    conn = get_conn()
    cur = conn.cursor()
    cols = ', '.join(columns)
    conflict = f"ON CONFLICT ({conflict_col}) DO NOTHING" if conflict_col else ""
    sql = f"INSERT INTO {table} ({cols}) VALUES %s {conflict}"
    execute_values(cur, sql, data, page_size=5000)
    conn.commit()
    conn.close()
    return len(data)

def get_lookup(table, id_col, val_col):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT {id_col}, {val_col} FROM {table}")
    result = {r[1]: r[0] for r in cur.fetchall()}
    conn.close()
    return result

def get_zona_lookup():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT zona_id, grid_lat, grid_lon FROM dim_zonas")
    result = {(r[1], r[2]): r[0] for r in cur.fetchall()}
    conn.close()
    return result

def get_max_timestamp():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT COALESCE(MAX(timestamp), '1970-01-01'::timestamptz) FROM fact_mediciones")
    result = cur.fetchone()[0]
    conn.close()
    return result.isoformat() if result else '1970-01-01'

# ============================================================================
# EXTRACCIÓN
# ============================================================================

def extract(watermark=None, limit=None):
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    data = []
    offset = 0
    batch = 10000
    
    while True:
        q = supabase.table('locations').select('*')
        if watermark and watermark != '1970-01-01':
            q = q.gt('timestamp', watermark)
        q = q.order('timestamp').range(offset, offset + batch - 1)
        
        res = q.execute().data
        if not res:
            break
        data.extend(res)
        print(f"   Lote {offset//batch + 1}: {len(res):,}")
        
        if limit and len(data) >= limit:
            return pd.DataFrame(data[:limit])
        if len(res) < batch:
            break
        offset += batch
    
    pdf = pd.DataFrame(data)
    print(f"   Columnas disponibles: {list(pdf.columns)}")
    return pdf

# ============================================================================
# ETL PRINCIPAL
# ============================================================================

def run(mode='incremental', limit=None):
    t0 = datetime.now()
    
    print("=" * 70)
    print("ETL FINAL OPTIMIZADO")
    print("=" * 70)
    
    # 1. EXTRACCIÓN
    print("\n[1/6] Extrayendo datos...")
    wm = None if mode == 'initial' else get_max_timestamp()
    pdf = extract(wm, limit)
    if pdf.empty:
        print("Sin datos nuevos")
        return
    print(f"   Total: {len(pdf):,}")
    
    # 2. SPARK DATAFRAME
    print("\n[2/6] Procesando con Spark...")
    df = spark.createDataFrame(pdf)
    df = df.filter(col("latitude").isNotNull() & col("longitude").isNotNull() & col("timestamp").isNotNull())
    df = df.withColumn("timestamp", to_timestamp(col("timestamp")))
    
    # NORMALIZACIONES CORREGIDAS basadas en datos reales
    
    # OPERADOR: ENTEL, Entel, Entel S.A Bolivia, LaDistanciaNosCuida, Movil GSM, BOMOV, +18VACUNATE -> ENTEL
    df = df.withColumn("operador_normalizado", 
        when(upper(col("sim_operator")).contains("ENTEL"), "ENTEL")
        .when(upper(col("sim_operator")).contains("DISTANCIA"), "ENTEL")  # LaDistanciaNosCuida
        .when(upper(col("sim_operator")).contains("MOVIL"), "ENTEL")      # Movil GSM
        .when(upper(col("sim_operator")).contains("BOMOV"), "ENTEL")
        .when(upper(col("sim_operator")).contains("VACUNATE"), "ENTEL")   # +18VACUNATE
        .when(upper(col("sim_operator")).contains("TIGO"), "TIGO")
        .when(upper(col("sim_operator")).contains("VIVA"), "VIVA")
        .when(upper(col("sim_operator")).contains("T-MOBILE"), "T-MOBILE")
        .when(col("sim_operator").isin("Sin señal", "None", "Unknown", ""), "SIN_SEÑAL")
        .when(col("sim_operator").isNull(), "SIN_SEÑAL")
        .otherwise(upper(col("sim_operator")))
    )
    
    # RED: Mobile, WiFi, 4G, 3G, 2G, None
    df = df.withColumn("red_normalizada",
        when(upper(col("network_type")) == "WIFI", "WIFI")
        .when(upper(col("network_type")) == "4G", "4G")
        .when(upper(col("network_type")) == "3G", "3G")
        .when(upper(col("network_type")) == "2G", "2G")
        .when(upper(col("network_type")) == "MOBILE", "MOBILE")
        .when(upper(col("network_type")).contains("LTE"), "4G")
        .when(col("network_type").isin("None", "", None), "SIN_RED")
        .when(col("network_type").isNull(), "SIN_RED")
        .otherwise(upper(col("network_type")))
    )
    
    df = df.withColumn("calidad_senal",
        when(col("signal") >= -70, "EXCELENTE")
        .when(col("signal") >= -85, "BUENA")
        .when(col("signal") >= -95, "REGULAR")
        .when(col("signal") >= -105, "MALA")
        .otherwise("CRITICA")
    )
    
    df = df.withColumn("grid_lat", (col("latitude") / GRID_SIZE).cast("integer"))
    df = df.withColumn("grid_lon", (col("longitude") / GRID_SIZE).cast("integer"))
    df = df.withColumn("zona_altitud", when(col("altitude") < 2500, "BAJA").when(col("altitude") < 3000, "MEDIA").otherwise("ALTA"))
    
    # Convertir velocidad de m/s a km/h
    df = df.withColumn("speed_kmh", col("speed") * 3.6)
    
    df = df.withColumn("sector_id", concat((col("latitude") * 10000).cast("integer"), lit("_"), (col("longitude") * 10000).cast("integer")))
    df = df.withColumn("fecha", to_date(col("timestamp")))
    df = df.withColumn("hora", hour(col("timestamp")))
    df = df.withColumn("tiempo_id", year(col("fecha")) * 10000 + month(col("fecha")) * 100 + dayofmonth(col("fecha")))
    
    df.cache()
    n = df.count()
    print(f"   Registros válidos: {n:,}")
    
    # 3. DIMENSIONES
    print("\n[3/6] Creando dimensiones...")
    
    # DIM_TIEMPO
    dt = df.select("fecha").distinct() \
        .withColumn("tiempo_id", year(col("fecha"))*10000 + month(col("fecha"))*100 + dayofmonth(col("fecha"))) \
        .withColumn("anio", year(col("fecha"))) \
        .withColumn("trimestre", quarter(col("fecha"))) \
        .withColumn("mes", month(col("fecha"))) \
        .withColumn("semana_anio", weekofyear(col("fecha"))) \
        .withColumn("dia_mes", dayofmonth(col("fecha"))) \
        .withColumn("dia_semana", dayofweek(col("fecha"))) \
        .withColumn("nombre_dia", when(col("dia_semana")==1,"DOMINGO").when(col("dia_semana")==2,"LUNES")
            .when(col("dia_semana")==3,"MARTES").when(col("dia_semana")==4,"MIERCOLES")
            .when(col("dia_semana")==5,"JUEVES").when(col("dia_semana")==6,"VIERNES").otherwise("SABADO")) \
        .withColumn("nombres_mes", when(col("mes")==1,"Enero").when(col("mes")==2,"Febrero")
            .when(col("mes")==3,"Marzo").when(col("mes")==4,"Abril").when(col("mes")==5,"Mayo")
            .when(col("mes")==6,"Junio").when(col("mes")==7,"Julio").when(col("mes")==8,"Agosto")
            .when(col("mes")==9,"Septiembre").when(col("mes")==10,"Octubre")
            .when(col("mes")==11,"Noviembre").otherwise("Diciembre")).toPandas()
    
    # DIM_HORA
    dh = df.select("hora").distinct() \
        .withColumn("hora_id", col("hora")) \
        .withColumn("franja_horaria", when(col("hora").between(6,11),"MAÑANA")
            .when(col("hora").between(12,18),"TARDE").when(col("hora").between(19,23),"NOCHE")
            .otherwise("MADRUGADA")).toPandas()
    
    # DIM_OPERADOR
    dop = df.select("operador_normalizado").distinct().filter(col("operador_normalizado").isNotNull()).toPandas()
    
    # DIM_RED
    dred = df.select("red_normalizada").distinct().filter(col("red_normalizada").isNotNull()).toPandas()
    
    # DIM_DISPOSITIVO - DISTINCT por device_name (59 únicos)
    # Generar device_id único cuando es NULL (basado en device_name)
    df = df.withColumn("device_id", 
        when(col("device_id").isNull() | (col("device_id") == ""), 
             concat(lit("UNKNOWN_"), col("device_name")))
        .otherwise(col("device_id"))
    )
    ddisp = df.select("device_id", "device_name").distinct().toPandas()
    print(f"   Dispositivos únicos: {len(ddisp)}")
    
    # DIM_ZONAS
    dzon = df.groupBy("grid_lat", "grid_lon").agg(
        count("*").alias("total_mediciones"), round(avg("latitude"),6).alias("centro_lat"),
        round(avg("longitude"),6).alias("centro_lon"), round(avg("altitude"),2).alias("altitud_promedio")) \
        .withColumn("zona_nombre", concat(lit("ZONA_"), col("grid_lat"), lit("_"), col("grid_lon"))) \
        .withColumn("grid_latitud_inicio", col("grid_lat") * GRID_SIZE) \
        .withColumn("grid_latitud_fin", (col("grid_lat") + 1) * GRID_SIZE) \
        .withColumn("grid_longitud_inicio", col("grid_lon") * GRID_SIZE) \
        .withColumn("grid_longitud_fin", (col("grid_lon") + 1) * GRID_SIZE).toPandas()
    
    # 4. INSERTAR DIMENSIONES
    print("\n[4/6] Insertando dimensiones...")
    
    # Tiempo
    data = [(int(r['tiempo_id']), r['fecha'], int(r['anio']), int(r['trimestre']), int(r['mes']),
             int(r['semana_anio']), int(r['dia_mes']), int(r['dia_semana']), r['nombre_dia'], r['nombres_mes'])
            for _, r in dt.iterrows()]
    batch_insert('dim_tiempo', data, ['tiempo_id','fecha','anio','trimestre','mes','semana_anio','dia_mes','dia_semana','nombre_dia','nombres_mes'], 'tiempo_id')
    print(f"   ✓ dim_tiempo: {len(data)}")
    
    # Hora
    data = [(int(r['hora_id']), int(r['hora']), r['franja_horaria']) for _, r in dh.iterrows()]
    batch_insert('dim_hora', data, ['hora_id','hora','franja_horaria'], 'hora_id')
    print(f"   ✓ dim_hora: {len(data)}")
    
    # Operador
    data = [(r['operador_normalizado'],) for _, r in dop.iterrows()]
    batch_insert('dim_operador', data, ['operador_nombre'], 'operador_nombre')
    print(f"   ✓ dim_operador: {len(data)}")
    
    # Red
    data = [(r['red_normalizada'], '4G' if r['red_normalizada'] in ['LTE','4G'] else ('3G' if r['red_normalizada']=='3G' else None))
            for _, r in dred.iterrows()]
    batch_insert('dim_red', data, ['red_tipo','generacion'], 'red_tipo')
    print(f"   ✓ dim_red: {len(data)}")
    
    # Dispositivo - usar device_id (UUID) como clave natural
    data = [(r['device_id'], r['device_name']) for _, r in ddisp.iterrows()]
    batch_insert('dim_dispositivo', data, ['device_id','device_name'], 'device_id')
    print(f"   ✓ dim_dispositivo: {len(data)}")
    
    # Zonas
    data = [(r['zona_nombre'], int(r['grid_lat']), int(r['grid_lon']), r['centro_lat'], r['centro_lon'],
             int(r['total_mediciones']), r['altitud_promedio'], r['grid_latitud_inicio'], r['grid_latitud_fin'],
             r['grid_longitud_inicio'], r['grid_longitud_fin']) for _, r in dzon.iterrows()]
    conn = get_conn()
    cur = conn.cursor()
    sql = """INSERT INTO dim_zonas (zona_nombre, grid_lat, grid_lon, centro_lat, centro_lon, total_mediciones, altitud_promedio,
             grid_latitud_inicio, grid_latitud_fin, grid_longitud_inicio, grid_longitud_fin) VALUES %s
             ON CONFLICT (grid_lat, grid_lon) DO UPDATE SET total_mediciones = dim_zonas.total_mediciones + EXCLUDED.total_mediciones"""
    execute_values(cur, sql, data, page_size=5000)
    conn.commit()
    conn.close()
    print(f"   ✓ dim_zonas: {len(data)}")
    
    # 5. LOOKUPS
    print("\n[5/6] Obteniendo lookups...")
    lk_op = get_lookup('dim_operador', 'operador_id', 'operador_nombre')
    lk_red = get_lookup('dim_red', 'red_id', 'red_tipo')
    lk_disp = get_lookup('dim_dispositivo', 'dispositivo_id', 'device_id')
    lk_zona = get_zona_lookup()
    lk_cal = {'EXCELENTE':1, 'BUENA':2, 'REGULAR':3, 'MALA':4, 'CRITICA':5}
    
    # 6. HECHOS
    print("\n[6/6] Insertando hechos...")
    
    fpd = df.select("timestamp", "tiempo_id", "hora", "device_id", "device_name", "operador_normalizado",
                    "red_normalizada", "calidad_senal", "zona_altitud", "grid_lat", "grid_lon",
                    "signal", "speed_kmh", "altitude", "battery", "latitude", "longitude", "id").toPandas()
    
    # Mapear IDs - todos los dispositivos tienen device_id válido ahora
    fpd['dispositivo_id'] = fpd['device_id'].map(lk_disp).fillna(0).astype(int)
    
    # Debug: verificar dispositivos sin mapeo
    failed_devices = fpd[fpd['dispositivo_id'] == 0]
    if len(failed_devices) > 0:
        print(f"   ⚠️  {len(failed_devices)} dispositivos no encontrados, insertando...")
        missing_devices = failed_devices[['device_id', 'device_name']].drop_duplicates()
        missing_data = [(r['device_id'], r['device_name']) for _, r in missing_devices.iterrows()]
        batch_insert('dim_dispositivo', missing_data, ['device_id','device_name'], 'device_id')
        print(f"   ✓ Insertados {len(missing_data)} dispositivos")
        # Actualizar lookup
        lk_disp = get_lookup('dim_dispositivo', 'dispositivo_id', 'device_id')
        fpd['dispositivo_id'] = fpd['device_id'].map(lk_disp).fillna(0).astype(int)
    
    fpd['operador_id'] = fpd['operador_normalizado'].map(lk_op).fillna(0).astype(int)
    fpd['red_id'] = fpd['red_normalizada'].map(lk_red).fillna(0).astype(int)
    fpd['calidad_id'] = fpd['calidad_senal'].map(lk_cal).fillna(3).astype(int)
    fpd['zona_id'] = fpd.apply(lambda r: lk_zona.get((int(r['grid_lat']), int(r['grid_lon'])), 0), axis=1)
    
    # Preparar datos
    data = [(r['timestamp'], int(r['tiempo_id']), int(r['hora']), int(r['dispositivo_id']), int(r['operador_id']),
             int(r['red_id']), int(r['calidad_id']), int(r['zona_id']), r['zona_altitud'],
             r['signal'], r['speed_kmh'], r['altitude'], r['battery'], r['latitude'], r['longitude'], str(r['id']))
            for _, r in fpd.iterrows()]
    
    # Insert masivo con ON CONFLICT para evitar duplicados
    conn = get_conn()
    cur = conn.cursor()
    sql = """INSERT INTO fact_mediciones (timestamp, tiempo_id, hora_id, dispositivo_id, operador_id, red_id,
             calidad_id, zona_id, zona_altitud, medida_senal, medida_velocidad, medida_altitud, medida_bateria,
             latitude, longitude, source_id) VALUES %s ON CONFLICT (source_id, timestamp) DO NOTHING"""
    
    bs = 10000
    total = len(data)
    for i in range(0, total, bs):
        execute_values(cur, sql, data[i:i+bs], page_size=bs)
        conn.commit()
        done = i + bs if i + bs < total else total
        print(f"   {done:,}/{total:,}")
    conn.close()
    
    # RESUMEN
    dur = (datetime.now() - t0).total_seconds()
    print("\n" + "=" * 70)
    print(f"✓ COMPLETADO: {len(data):,} registros en {dur:.1f}s ({len(data)/dur:.0f} reg/s)")
    print("=" * 70)
    
    df.unpersist()
    spark.stop()

# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else 'incremental'
    limit = int(sys.argv[2]) if len(sys.argv) > 2 else None
    run(mode, limit)
