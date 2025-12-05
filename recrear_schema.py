"""
Script para RECREAR el schema completo en Supabase
ADVERTENCIA: Esto eliminará TODOS los datos existentes
"""
import psycopg2

pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

print("=" * 80)
print("RECREAR SCHEMA EN SUPABASE")
print("=" * 80)
print("\n⚠️  ADVERTENCIA: Esto eliminará TODOS los datos existentes!")
respuesta = input("\n¿Estás seguro? Escribe 'SI' para continuar: ")

if respuesta.strip().upper() != 'SI':
    print("\n❌ Operación cancelada")
    exit(0)

try:
    conn = psycopg2.connect(**pg_config)
    conn.autocommit = True
    cursor = conn.cursor()
    
    print("\n📖 Leyendo schema_optimizado.sql...")
    with open('schema_optimizado.sql', 'r', encoding='utf-8') as f:
        sql_script = f.read()
    
    print("\n🗑️  Ejecutando DROP de tablas existentes...")
    print("🏗️  Creando nuevas tablas...")
    print("📊 Insertando datos de referencia...")
    
    # Ejecutar el script completo
    cursor.execute(sql_script)
    
    # Verificar tablas creadas
    cursor.execute("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' 
        AND table_type = 'BASE TABLE'
        ORDER BY table_name
    """)
    
    tablas = cursor.fetchall()
    
    print("\n✅ Schema recreado exitosamente!")
    print(f"\n📋 Tablas creadas ({len(tablas)}):")
    for tabla in tablas:
        print(f"   ✓ {tabla[0]}")
    
    # Verificar datos de referencia
    cursor.execute("SELECT COUNT(*) FROM dim_calidad")
    n_calidad = cursor.fetchone()[0]
    
    print(f"\n📊 Datos de referencia insertados:")
    print(f"   ✓ dim_calidad: {n_calidad} categorías")
    
    conn.close()
    
    print("\n" + "=" * 80)
    print("✅ SCHEMA LISTO PARA ETL")
    print("=" * 80)
    print("\nPróximos pasos:")
    print("1. Ejecuta: python etl_final.py")
    print("2. O ejecuta: python run_etl_incremental.py")
    
except Exception as e:
    print(f"\n❌ Error: {str(e)}")
    import traceback
    traceback.print_exc()
