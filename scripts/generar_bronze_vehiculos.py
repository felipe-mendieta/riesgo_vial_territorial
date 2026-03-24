import pandas as pd
import os
import sys
from utils import get_logger, safe_read_csv

logger = get_logger("generar_bronze_vehiculos")

input_file = r"..\raw\inec_transporte\vehiculos_2023\2023_VEHICULOS_DATOS_ABIERTOS\2023_VEHICULOS_DATOS ABIERTOS\2023_bdd_vehiculos_matriculados.csv"
output_dir = r"..\bronze"

# Create output dir if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read CSV
logger.info(f"Reading from {input_file} ...")
df = safe_read_csv(input_file, sep=";", decimal=",", low_memory=False, logger=logger)

if df is None or df.empty:
    logger.error("No se pudo obtener datos válidos del CSV de vehículos.")
    sys.exit(1)

csv_out = os.path.join(output_dir, "brz_vehiculos_matriculados_2023.csv")
parquet_out = os.path.join(output_dir, "brz_vehiculos_matriculados_2023.parquet")

logger.info(f"Writing to {csv_out} ...")
df.to_csv(csv_out, index=False)

logger.info(f"Writing to {parquet_out} ...")
df.to_parquet(parquet_out, index=False)

logger.info("Conversion complete.")
