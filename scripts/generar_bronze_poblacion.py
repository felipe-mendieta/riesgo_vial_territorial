import pandas as pd
import os
import sys
from datetime import datetime, timezone
from utils import get_logger, safe_read_excel

logger = get_logger("generar_bronze_poblacion")

# Mapeo de hojas a nombre oficial de provincia
# Basado en los estándares de las hojas del archivo Excel
PROVINCES_DPA = {
    'azuay_n': 'Azuay',
    'bolivar_n': 'Bolívar',
    'cañar_n': 'Cañar',
    'carchi_n': 'Carchi',
    'cotopaxi_n': 'Cotopaxi',
    'chimborazo_n': 'Chimborazo',
    'el_oro_n': 'El Oro',
    'esmeraldas_n': 'Esmeraldas',
    'guayas_n': 'Guayas',
    'imbabura_n': 'Imbabura',
    'loja_n': 'Loja',
    'los_rios_n': 'Los Ríos',
    'manabi_n': 'Manabí',
    'morona_n': 'Morona Santiago',
    'napo_n': 'Napo',
    'pastaza_n': 'Pastaza',
    'pichincha_n': 'Pichincha',
    'tungurahua_n': 'Tungurahua',
    'zamora_n': 'Zamora Chinchipe',
    'galapagos_n': 'Galápagos',
    'sucumbios_n': 'Sucumbíos',
    'orellana_n': 'Orellana',
    'santo_domingo_n': 'Santo Domingo de los Tsáchilas',
    'santa_elena_n': 'Santa Elena'
}

input_file = r'..\raw\inec_poblacion\1_TABULADOS\2_PROVINCIAL\Tabulado_provincial_edad_simple_1990-2035_rev2024.xlsx'
output_dir = r'..\bronze'

csv_out = os.path.join(output_dir, "brz_poblacion_provincias.csv")
parquet_out = os.path.join(output_dir, "brz_poblacion_provincias.parquet")

os.makedirs(output_dir, exist_ok=True)

results = []
timestamp_utc = datetime.now(timezone.utc).isoformat()

logger.info(f"Reading from {input_file} ...")
logger.info("This may take a few seconds...")
xl = safe_read_excel(input_file, sheet_name=None, logger=logger)

if xl is None:
    logger.error("No se pudo obtener el Excel de población.")
    sys.exit(1)

for sheet_name, prov_name in PROVINCES_DPA.items():
    if sheet_name not in xl.sheet_names:
        logger.warning(f"Warning: Sheet {sheet_name} not found.")
        continue
    
    # Leer las primeras 30 filas para buscar la cabecera y la fila 'Total'
    df = xl.parse(sheet_name, header=None, nrows=30)
    
    year_row_idx = None
    col_2021 = None
    col_2023 = None
    
    for idx, row in df.iterrows():
        row_str = " ".join([str(x) for x in row.values])
        if '2021' in row_str and '2023' in row_str:
            year_row_idx = idx
            for c_idx, val in enumerate(row.values):
                if str(val) == '2021.0' or str(val) == '2021':
                    col_2021 = c_idx
                if str(val) == '2023.0' or str(val) == '2023':
                    col_2023 = c_idx
            break
            
    if year_row_idx is None or col_2021 is None or col_2023 is None:
        logger.warning(f"Could not find year row or required columns in {sheet_name}")
        continue

    total_row_idx = None
    for idx, row in df.iterrows():
        if idx <= year_row_idx:
            continue
        row_start = str(row[0]).strip() + " " + str(row[1]).strip() + " " + str(row[2]).strip()
        if 'Total' in row_start:
            total_row_idx = idx
            break
            
    if total_row_idx is None:
        logger.warning(f"Could not find 'Total' row in {sheet_name}")
        continue
        
    pop_2021 = int(float(df.iloc[total_row_idx, col_2021]))
    pop_2023 = int(float(df.iloc[total_row_idx, col_2023]))
    
    # Extraer exactamente los campos solicitados por el usuario
    results.append({
        'provincia': prov_name,
        'hoja_origen': sheet_name,
        'poblacion_2023': pop_2023,
        'poblacion_2021': pop_2021,
        'fecha_procesamiento_utc': timestamp_utc
    })

if not results:
    logger.error("No se pudo procesar ninguna provincia del Excel.")
    sys.exit(1)

logger.info(f"Processed {len(results)} provinces.")

df_final = pd.DataFrame(results)

logger.info(f"Writing to {csv_out} ...")
df_final.to_csv(csv_out, index=False, encoding="utf-8")

logger.info(f"Writing to {parquet_out} ...")
df_final.to_parquet(parquet_out, index=False)

logger.info("Conversion complete.")
