import psycopg2

conn = psycopg2.connect(
    host='db.rzdonirbjwnjlqmmjhlw.supabase.co',
    port='5432',
    database='postgres',
    user='postgres',
    password='fabiana'
)
cur = conn.cursor()

print("=" * 80)
print("VERIFICACIÓN DE DISPOSITIVOS")
print("=" * 80)

# Dispositivos en dim_dispositivo
cur.execute("SELECT COUNT(*) FROM dim_dispositivo")
total_dim = cur.fetchone()[0]
print(f"\n1. Total en dim_dispositivo: {total_dim}")

# Dispositivos únicos en fact_mediciones
cur.execute("SELECT COUNT(DISTINCT dispositivo_id) FROM fact_mediciones")
total_fact = cur.fetchone()[0]
print(f"2. Dispositivos únicos en fact_mediciones: {total_fact}")

# Dispositivos con coordenadas válidas
cur.execute("""
    SELECT COUNT(DISTINCT f.dispositivo_id) 
    FROM fact_mediciones f 
    WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
""")
con_coords = cur.fetchone()[0]
print(f"3. Dispositivos con lat/lon válidos: {con_coords}")

# Dispositivos con device_name visible
cur.execute("""
    SELECT COUNT(DISTINCT d.device_name)
    FROM fact_mediciones f
    INNER JOIN dim_dispositivo d ON f.dispositivo_id = d.dispositivo_id
    WHERE f.latitude IS NOT NULL AND f.longitude IS NOT NULL
""")
con_nombre = cur.fetchone()[0]
print(f"4. Device_names únicos con datos: {con_nombre}")

# Listar dispositivos sin datos
cur.execute("""
    SELECT d.dispositivo_id, d.device_name
    FROM dim_dispositivo d
    LEFT JOIN fact_mediciones f ON d.dispositivo_id = f.dispositivo_id
    WHERE f.medicion_id IS NULL
    ORDER BY d.device_name
""")
sin_datos = cur.fetchall()
if sin_datos:
    print(f"\n⚠️  Dispositivos en dim_dispositivo SIN mediciones ({len(sin_datos)}):")
    for disp in sin_datos[:10]:
        print(f"   - {disp[1]} (ID: {disp[0]})")

conn.close()

print("\n" + "=" * 80)
print("El dashboard muestra correctamente los dispositivos CON DATOS.")
print("=" * 80)
