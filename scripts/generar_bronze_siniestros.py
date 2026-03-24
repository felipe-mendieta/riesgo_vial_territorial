import pandas as pd
import os
import sys
from utils import get_logger, safe_read_csv

logger = get_logger("generar_bronze_siniestros")

input_file = r"..\raw\inec_transporte\siniestros_2021\2021_SINIESTROS_DATOS_ABIERTOS\2021_SINIESTROS_DATOS_ABIERTOS\INEC_Anuario de Estadísticas de Transporte_Siniestros de Tránsito_BDD_2021.csv.csv"
output_dir = r"..\bronze"

# Create output dir if it doesn't exist
os.makedirs(output_dir, exist_ok=True)

# Read CSV
logger.info(f"Reading from {input_file} ...")
df = safe_read_csv(input_file, sep=";", logger=logger)

if df is None or df.empty:
    logger.error("No se pudo obtener datos válidos del CSV de siniestros.")
    sys.exit(1)

# Normalizing column names: lower case, remove accents/spaces if any? The instruction doesn't say, but it's good practice. I'll just write it directly as requested unless they want further transformations.
# The user asked: "quiero tener la capa bronze ... quisiera tenerlos en formato parquet y csv ... usa este formato brz_siniestros_transito_2021.csv"
# I'll just save them as is.

csv_out = os.path.join(output_dir, "brz_siniestros_transito_2021.csv")
parquet_out = os.path.join(output_dir, "brz_siniestros_transito_2021.parquet")

logger.info(f"Writing to {csv_out} ...")
df.to_csv(csv_out, index=False)

logger.info(f"Writing to {parquet_out} ...")
df.to_parquet(parquet_out, index=False)

logger.info("Conversion complete.")
