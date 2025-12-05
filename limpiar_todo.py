"""
Limpieza completa de la base de datos - Elimina TODAS las tablas, vistas, índices
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
print("LIMPIEZA COMPLETA DE BASE DE DATOS")
print("=" * 80)
print("\n⚠️  ADVERTENCIA: Esto eliminará TODO en el schema public!\n")

respuesta = input("¿Estás seguro? Escribe 'SI' para continuar: ")

if respuesta.upper() != 'SI':
    print("❌ Cancelado")
    exit()

print("\n🗑️  Eliminando TODO...")

conn = psycopg2.connect(**pg_config)
cur = conn.cursor()

# 1. Eliminar todas las vistas materializadas
print("\n📊 Eliminando vistas materializadas...")
cur.execute("""
    SELECT matviewname FROM pg_matviews WHERE schemaname='public'
""")
matviews = [r[0] for r in cur.fetchall()]
for mv in matviews:
    print(f"   DROP MATERIALIZED VIEW {mv}")
    cur.execute(f"DROP MATERIALIZED VIEW IF EXISTS {mv} CASCADE")
conn.commit()

# 2. Eliminar todas las vistas
print("\n👁️  Eliminando vistas...")
cur.execute("""
    SELECT viewname FROM pg_views WHERE schemaname='public'
""")
views = [r[0] for r in cur.fetchall()]
for view in views:
    print(f"   DROP VIEW {view}")
    cur.execute(f"DROP VIEW IF EXISTS {view} CASCADE")
conn.commit()

# 3. Eliminar todas las tablas
print("\n📋 Eliminando tablas...")
cur.execute("""
    SELECT tablename FROM pg_tables WHERE schemaname='public' 
    ORDER BY tablename
""")
tables = [r[0] for r in cur.fetchall()]
for table in tables:
    print(f"   DROP TABLE {table}")
    cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
conn.commit()

# 4. Eliminar todas las funciones
print("\n⚙️  Eliminando funciones...")
cur.execute("""
    SELECT routine_name FROM information_schema.routines 
    WHERE routine_schema='public' AND routine_type='FUNCTION'
""")
functions = [r[0] for r in cur.fetchall()]
for func in functions:
    print(f"   DROP FUNCTION {func}")
    cur.execute(f"DROP FUNCTION IF EXISTS {func} CASCADE")
conn.commit()

# 5. Eliminar todas las secuencias huérfanas
print("\n🔢 Eliminando secuencias...")
cur.execute("""
    SELECT sequence_name FROM information_schema.sequences 
    WHERE sequence_schema='public'
""")
sequences = [r[0] for r in cur.fetchall()]
for seq in sequences:
    print(f"   DROP SEQUENCE {seq}")
    cur.execute(f"DROP SEQUENCE IF EXISTS {seq} CASCADE")
conn.commit()

conn.close()

print("\n" + "=" * 80)
print("✅ BASE DE DATOS COMPLETAMENTE LIMPIA")
print("=" * 80)
print("\nAhora ejecuta: python recrear_schema.py")
