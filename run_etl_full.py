"""
Script para ejecutar ETL completo desde cero
==========================================
Procesa TODOS los datos de Supabase
"""

from etl_final import run

if __name__ == "__main__":
    print("Iniciando ETL COMPLETO desde cero...")
    print("Se procesarán TODOS los datos de Supabase")
    print("⚠️  Esto puede tomar mucho tiempo")
    print("-" * 50)
    
    # Ejecutar en modo inicial (todos los datos)
    run(mode='initial')
    
    print("-" * 50)
    print("ETL completo finalizado")
    print("\nPara procesar solo datos nuevos:")
    print("python run_etl_incremental.py")
