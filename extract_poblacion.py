import pandas as pd
from datetime import datetime, timezone

# Mapeo de hojas a código DPA y nombre de provincia
PROVINCES_DPA = {
    'azuay_n': ('01', 'Azuay'),
    'bolivar_n': ('02', 'Bolívar'),
    'cañar_n': ('03', 'Cañar'),
    'carchi_n': ('04', 'Carchi'),
    'cotopaxi_n': ('05', 'Cotopaxi'),
    'chimborazo_n': ('06', 'Chimborazo'),
    'el_oro_n': ('07', 'El Oro'),
    'esmeraldas_n': ('08', 'Esmeraldas'),
    'guayas_n': ('09', 'Guayas'),
    'imbabura_n': ('10', 'Imbabura'),
    'loja_n': ('11', 'Loja'),
    'los_rios_n': ('12', 'Los Ríos'),
    'manabi_n': ('13', 'Manabí'),
    'morona_n': ('14', 'Morona Santiago'),
    'napo_n': ('15', 'Napo'),
    'pastaza_n': ('16', 'Pastaza'),
    'pichincha_n': ('17', 'Pichincha'),
    'tungurahua_n': ('18', 'Tungurahua'),
    'zamora_n': ('19', 'Zamora Chinchipe'),
    'galapagos_n': ('20', 'Galápagos'),
    'sucumbios_n': ('21', 'Sucumbíos'),
    'orellana_n': ('22', 'Orellana'),
    'santo_domingo_n': ('23', 'Santo Domingo de los Tsáchilas'),
    'santa_elena_n': ('24', 'Santa Elena')
}

file_path = r'c:\Projects\riesgo_vial_territorial\raw\inec_poblacion\1_TABULADOS\2_PROVINCIAL\Tabulado_provincial_edad_simple_1990-2035_rev2024.xlsx'
output_csv = r'c:\Projects\riesgo_vial_territorial\raw\poblacion_provincias_2021_2023.csv'
output_parquet = r'c:\Projects\riesgo_vial_territorial\raw\poblacion_provincias_2021_2023.parquet'

results = []
timestamp_utc = datetime.now(timezone.utc).isoformat()

print("Cargando el archivo de Excel (puede tomar unos segundos)...")
xl = pd.ExcelFile(file_path)

for sheet_name, (dpa_code, prov_name) in PROVINCES_DPA.items():
    if sheet_name not in xl.sheet_names:
        print(f"Advertencia: No se encontró la hoja {sheet_name}")
        continue
    
    # Leemos suficientes filas para encontrar el header y la fila Total
    df = xl.parse(sheet_name, header=None, nrows=30)
    
    # Buscar dinámicamente la fila donde esté 2021 y 2023
    year_row_idx = None
    col_2021 = None
    col_2023 = None
    
    for idx, row in df.iterrows():
        # Verificamos si en la fila de alguna manera está 2021 y 2023
        row_str = " ".join([str(x) for x in row.values])
        if '2021' in row_str and '2023' in row_str:
            year_row_idx = idx
            # Encontrar índices de columna exactos
            for c_idx, val in enumerate(row.values):
                if str(val) == '2021.0' or str(val) == '2021':
                    col_2021 = c_idx
                if str(val) == '2023.0' or str(val) == '2023':
                    col_2023 = c_idx
            break
            
    if year_row_idx is None or col_2021 is None or col_2023 is None:
        print(f"No se encontró la fila de años o las columnas requeridas en {sheet_name}")
        continue

    # Buscar dinámicamente la fila "Total" en la columna de edades (usualmente col 1 o 2)
    total_row_idx = None
    for idx, row in df.iterrows():
        if idx <= year_row_idx:
            continue
        # Buscamos 'Total' en las primeras columnas
        row_start = str(row[0]).strip() + " " + str(row[1]).strip() + " " + str(row[2]).strip()
        if 'Total' in row_start:
            total_row_idx = idx
            break
            
    if total_row_idx is None:
        print(f"No se encontró la fila 'Total' en {sheet_name}")
        continue
        
    pop_2021 = int(float(df.iloc[total_row_idx, col_2021]))
    pop_2023 = int(float(df.iloc[total_row_idx, col_2023]))
    
    results.append({
        'cod_provincia': dpa_code,
        'provincia': prov_name,
        'hoja_origen': sheet_name,
        'poblacion_2023': pop_2023,
        'poblacion_2021': pop_2021,
        'fecha_procesamiento_utc': timestamp_utc
    })
    print(f"Procesado: {prov_name} -> 2021: {pop_2021}, 2023: {pop_2023}")

df_final = pd.DataFrame(results)

# Guardar a disco
print(f"\nGuardando archivos CSV y Parquet en formato raw...")
df_final.to_csv(output_csv, index=False)
try:
    df_final.to_parquet(output_parquet, index=False)
except Exception as e:
    print(f"Error al guardar Parquet (quizá falta pyarrow): {e}")
    # Install pyarrow automatically if failed
    import os
    os.system("pip install pyarrow")
    df_final.to_parquet(output_parquet, index=False)

print(f"\nProceso completado.")
print(f"Se crearon {len(df_final)} registros.")
print(f" - CSV: {output_csv}")
print(f" - Parquet: {output_parquet}")
print(df_final.head())
