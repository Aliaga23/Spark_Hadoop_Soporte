"""
Refrescar vistas materializadas para análisis ultra-rápido
"""
import psycopg2
from datetime import datetime

pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

print("=" * 80)
print("REFRESCANDO VISTAS MATERIALIZADAS")
print("=" * 80)

conn = psycopg2.connect(**pg_config)
cur = conn.cursor()

# 1. Verificar distribución de datos por partición
print("\n📊 Distribución de datos por partición:")
cur.execute("""
    SELECT 
        schemaname,
        tablename,
        pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
        (SELECT COUNT(*) FROM fact_mediciones WHERE 
            timestamp >= CASE 
                WHEN tablename = 'fact_mediciones_2025_11_w1' THEN '2025-11-14'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_11_w2' THEN '2025-11-21'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_11_w3' THEN '2025-11-28'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_12_w1' THEN '2025-12-05'::timestamptz
            END
            AND timestamp < CASE 
                WHEN tablename = 'fact_mediciones_2025_11_w1' THEN '2025-11-21'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_11_w2' THEN '2025-11-28'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_11_w3' THEN '2025-12-05'::timestamptz
                WHEN tablename = 'fact_mediciones_2025_12_w1' THEN '2025-12-07'::timestamptz
            END
        ) as registros
    FROM pg_tables 
    WHERE tablename LIKE 'fact_mediciones_2025%'
    ORDER BY tablename
""")
for row in cur.fetchall():
    print(f"   {row[1]}: {row[3]:,} registros ({row[2]})")

# 2. Refrescar vistas materializadas
print("\n🔄 Refrescando vistas materializadas...")

vistas = [
    ('mv_stats_diarias_operador', 'Estadísticas diarias por operador'),
    ('mv_stats_zonas', 'Estadísticas por zona geográfica'),
    ('mv_top_dispositivos', 'Top dispositivos')
]

for vista, descripcion in vistas:
    print(f"\n   {descripcion}...")
    t0 = datetime.now()
    cur.execute(f"REFRESH MATERIALIZED VIEW {vista}")
    conn.commit()
    dur = (datetime.now() - t0).total_seconds()
    
    # Contar registros
    cur.execute(f"SELECT COUNT(*) FROM {vista}")
    count = cur.fetchone()[0]
    print(f"   ✓ {vista}: {count:,} registros ({dur:.2f}s)")

# 3. Estadísticas generales
print("\n📈 Estadísticas generales:")
cur.execute("""
    SELECT 
        COUNT(*) as total_mediciones,
        COUNT(DISTINCT dispositivo_id) as dispositivos,
        COUNT(DISTINCT zona_id) as zonas,
        MIN(timestamp) as fecha_inicio,
        MAX(timestamp) as fecha_fin,
        ROUND(AVG(medida_senal)::numeric, 2) as senal_promedio
    FROM fact_mediciones
""")
stats = cur.fetchone()
print(f"   Total mediciones: {stats[0]:,}")
print(f"   Dispositivos únicos: {stats[1]:,}")
print(f"   Zonas cubiertas: {stats[2]:,}")
print(f"   Período: {stats[3]} → {stats[4]}")
print(f"   Señal promedio: {stats[5]} dBm")

conn.close()

print("\n" + "=" * 80)
print("✅ VISTAS MATERIALIZADAS ACTUALIZADAS")
print("=" * 80)
print("\nAhora el dashboard consultará datos pre-agregados (100-1000x más rápido)")
