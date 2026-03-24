import pandas as pd
import json

file_path = r'c:\Projects\riesgo_vial_territorial\raw\inec_poblacion\1_TABULADOS\2_PROVINCIAL\Tabulado_provincial_edad_simple_1990-2035_rev2024.xlsx'
xl = pd.ExcelFile(file_path)

# Dump sheet names to a text file
with open('sheet_names.txt', 'w', encoding='utf-8') as f:
    for name in xl.sheet_names:
        f.write(name + '\n')

# Parse azuay_n (first 30 rows) to see the structure
df = xl.parse('azuay_n', nrows=30)
df.to_csv('azuay_sample.csv', index=False)
