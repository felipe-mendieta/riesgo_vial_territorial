import os
import subprocess
import time
from utils import get_logger

logger = get_logger("run_all_pipeline")

# Lista de scripts en orden de ejecución del pipeline (ETL completo)
PIPELINE_SCRIPTS = [
    # 1. Capa Bronze (Extracción de las fuentes Raw a Parquet/CSV en Bronze)
    "generar_bronze_provincias.py",
    "generar_bronze_poblacion.py",
    "generar_bronze_siniestros.py",
    "generar_bronze_vehiculos.py",
    
    # 2. Capa Silver Standardized (Estandarización de columnas, tipos y textos)
    "generar_silver_standardized.py",
    
    # 3. Capa Silver Integrated (Agrupaciones clave y validación con dimensiones transversales)
    "build_silver_integrated.py",
    
    # 4. Capa Gold (Métricas de valor final para el negocio, rankings, outliers, proyecciones)
    "build_gold.py"
]

def run_pipeline():
    logger.info("=" * 60)
    logger.info("🚗 INICIANDO PIPELINE DE RIESGO VIAL TERRITORIAL (ETL MASTER) 🚗")
    logger.info("=" * 60)
    
    start_time_global = time.time()
    
    script_dir = os.path.dirname(os.path.abspath(__file__))
    failed_scripts = []
    
    for script_name in PIPELINE_SCRIPTS:
        script_path = os.path.join(script_dir, script_name)
        
        logger.info(f"\n[{time.strftime('%H:%M:%S')}] Ejecutando: {script_name}...")
        
        # Validar que el script exista
        if not os.path.exists(script_path):
            logger.error(f"❌ Error: El script {script_name} no fue encontrado en {script_dir}.")
            failed_scripts.append(script_name)
            continue
        
        # Ejecutar
        try:
            start_time = time.time()
            result = subprocess.run(
                ['python', script_name],
                cwd=script_dir,
                check=True,
                capture_output=False # Permitir que los print logs originales salgan por consola
            )
            elapsed = time.time() - start_time
            logger.info(f"✅ {script_name} completado exitosamente en {elapsed:.2f} segundos.")
            
        except subprocess.CalledProcessError as e:
            logger.error(f"❌ Falló la ejecución de {script_name}. (Exit code: {e.returncode})")
            failed_scripts.append(script_name)
            
    total_elapsed = time.time() - start_time_global
    logger.info("\n" + "=" * 60)
    logger.info(f"🌟 PIPELINE COMPLETADO EN {total_elapsed:.2f} SEGUNDOS 🌟")
    
    successful_count = len(PIPELINE_SCRIPTS) - len(failed_scripts)
    logger.info(f"Resumen: {successful_count} scripts exitosos, {len(failed_scripts)} fallidos.")
    
    if failed_scripts:
        logger.warning(f"Scripts fallidos: {', '.join(failed_scripts)}")
        
    logger.info("Capa Bronze, Silver y Gold actualizadas (según disponibilidad de datos).")
    logger.info("=" * 60)

if __name__ == "__main__":
    run_pipeline()
