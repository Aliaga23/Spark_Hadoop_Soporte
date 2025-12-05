import psycopg2
import pandas as pd

pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

conn = psycopg2.connect(**pg_config)

# Probar query simple
query = """
SELECT 
    f.medicion_id,
    o.operador_nombre,
    r.red_tipo,
    c.calidad_categoria
FROM fact_mediciones f
LEFT JOIN dim_operador o ON f.operador_id = o.operador_id
LEFT JOIN dim_red r ON f.red_id = r.red_id
LEFT JOIN dim_calidad c ON f.calidad_id = c.calidad_id
LIMIT 5
"""

df = pd.read_sql_query(query, conn)
print("Columnas obtenidas:")
print(df.columns.tolist())
print("\nPrimeras filas:")
print(df.head())

conn.close()
