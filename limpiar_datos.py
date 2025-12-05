import psycopg2

# Configuración PostgreSQL
pg_config = {
    'host': 'db.rzdonirbjwnjlqmmjhlw.supabase.co',
    'port': '5432',
    'database': 'postgres',
    'user': 'postgres',
    'password': 'fabiana'
}

def limpiar_todas_las_tablas():
    """Limpiar todos los datos de las tablas del data warehouse"""
    try:
        conn = psycopg2.connect(**pg_config)
        cursor = conn.cursor()
        
        print("Conectado a PostgreSQL - Limpiando datos...")
        
        # Lista de tablas en orden correcto (fact primero por foreign keys)
        tablas = [
            'fact_mediciones',
            'dim_zonas', 
            'dim_dispositivo',
            'dim_ubicacion',
            'dim_calidad',
            'dim_red',
            'dim_operador',
            'dim_hora',
            'dim_tiempo'
        ]
        
        for tabla in tablas:
            try:
                # Contar registros antes de limpiar
                cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
                count = cursor.fetchone()[0]
                
                # Truncate table (más rápido que DELETE)
                cursor.execute(f"TRUNCATE TABLE {tabla} RESTART IDENTITY CASCADE")
                conn.commit()
                
                print(f"✓ {tabla}: {count:,} registros eliminados")
                
            except Exception as e:
                print(f"✗ Error limpiando {tabla}: {str(e)}")
                conn.rollback()
        
        # Verificar que todas las tablas estén vacías
        print("\nVerificando que las tablas estén vacías...")
        for tabla in tablas:
            cursor.execute(f"SELECT COUNT(*) FROM {tabla}")
            count = cursor.fetchone()[0]
            print(f"  {tabla}: {count} registros")
        
        conn.close()
        print("\n✓ Limpieza completada - Todas las tablas vacías")
        
    except Exception as e:
        print(f"✗ Error general: {str(e)}")
        if 'conn' in locals():
            conn.close()

if __name__ == "__main__":
    limpiar_todas_las_tablas()
