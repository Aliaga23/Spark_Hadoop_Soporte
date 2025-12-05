import psycopg2

pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

try:
    conn = psycopg2.connect(**pg_config)
    cursor = conn.cursor()
    
    # Columnas de fact_mediciones
    cursor.execute("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'fact_mediciones' 
        ORDER BY ordinal_position
    """)
    columns = cursor.fetchall()
    print("\n📋 COLUMNAS DE FACT_MEDICIONES:")
    for col_name, col_type in columns:
        print(f"   - {col_name} ({col_type})")
    
    conn.close()
    print("\n" + "=" * 60)
    
except Exception as e:
    print(f"❌ Error: {str(e)}")
