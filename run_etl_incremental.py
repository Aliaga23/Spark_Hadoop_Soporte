"""
Script para ejecutar ETL incremental
====================================
Solo procesa datos nuevos desde la última ejecución
"""

from etl_final import run

if __name__ == "__main__":
    print("Iniciando ETL incremental...")
    print("Solo se procesarán datos nuevos desde la última ejecución")
    print("-" * 50)
    
    # Ejecutar en modo incremental (solo datos nuevos)
    run(mode='incremental')
    
    print("-" * 50)
    print("ETL incremental completado")
    print("\nPara procesar TODOS los datos desde cero:")
    print("python run_etl_full.py")
