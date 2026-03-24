import pandas as pd
import os
import sys
from utils import get_logger, safe_read_excel

logger = get_logger("generar_bronze_provincias")

input_file = r"..\raw\inec_geografia\CLASIFICADOR_GEOGRAFICO_2024\CODIFICACIÓN_2024.xlsx"
output_dir = r"..\bronze"

csv_out = os.path.join(output_dir, "brz_dim_provincia_dpa_2024.csv")
parquet_out = os.path.join(output_dir, "brz_dim_provincia_dpa_2024.parquet")

os.makedirs(output_dir, exist_ok=True)

logger.info(f"Reading from {input_file} (PROVINCIAS) ...")
# use dtype=str to preserve leading zeros like '01'
df = safe_read_excel(input_file, sheet_name="PROVINCIAS", skiprows=1, dtype=str, logger=logger)

if df is None or df.empty:
    logger.error("No se pudo obtener datos válidos del Excel de provincias.")
    sys.exit(1)

# Drop completely empty columns and rows
df = df.dropna(how='all', axis=1)
df = df.dropna(how='all', axis=0)

logger.info(f"Writing to {csv_out} ...")
df.to_csv(csv_out, index=False, encoding="utf-8")

logger.info(f"Writing to {parquet_out} ...")
df.to_parquet(parquet_out, index=False)

logger.info("Conversion complete.")
