import os
import sys
import pandas as pd
import numpy as np
from datetime import datetime, timezone
from utils import get_logger, safe_read_parquet

logger = get_logger("build_gold")

# Rutas
STD_DIR = r"..\silver\standardized"
INT_DIR = r"..\silver\integrated"
GLD_DIR = r"..\gold"

os.makedirs(GLD_DIR, exist_ok=True)
TIMESTAMP_UTC = datetime.now(timezone.utc).isoformat()

logger.info(">>> INICIANDO CONSTRUCCIÓN DE CAPA GOLD <<<")

# ==============================================================================
# 0. LECTURA DE FUENTES INTEGRATED Y STANDARDIZED
# ==============================================================================
cobertura_file = os.path.join(INT_DIR, "int_cobertura_llave_provincia.parquet")
siniestralidad_file = os.path.join(INT_DIR, "int_siniestralidad_por_provincia_2021.parquet")
siniestros_file = os.path.join(STD_DIR, "std_siniestros_evento_2021.parquet")

df_cobertura = safe_read_parquet(cobertura_file, logger)
if df_cobertura is None:
    logger.error("Tabla cobertura es mandatoria. Abortando Capa Gold.")
    sys.exit(1)

df_siniestralidad = safe_read_parquet(siniestralidad_file, logger)
if df_siniestralidad is None:
    logger.warning("Falta tabla siniestralidad. Se usarán valores 0.")
    df_siniestralidad = pd.DataFrame(columns=['cod_provincia', 'total_fallecidos', 'total_lesionados', 'total_victimas'])

df_siniestros = safe_read_parquet(siniestros_file, logger)
if df_siniestros is None:
    logger.warning("Falta detalle de siniestros. La tabla de causas estará vacía.")
    df_siniestros = pd.DataFrame(columns=['cod_provincia', 'desc_provincia', 'causa_siniestro'])

# Join para obtener el detalle analítico (total_fallecidos) en el master
df_fact = df_cobertura.merge(
    df_siniestralidad[['cod_provincia', 'total_fallecidos', 'total_lesionados', 'total_victimas']],
    on='cod_provincia',
    how='left'
)

# Rellenar ceros por si hay provincias sin esos datos
df_fact['total_fallecidos'] = df_fact['total_fallecidos'].fillna(0)
df_fact['total_lesionados'] = df_fact['total_lesionados'].fillna(0)
df_fact['total_victimas'] = df_fact['total_victimas'].fillna(0)

# Filtrar provincias no aplicables como 'ZONA EN ESTUDIO' (si no tienen población/parque)
# o mantenerla dejando nulos. Quitaremos filas con Poblacion 0 para evitar divisiones por cero.
df_fact = df_fact[df_fact['poblacion_2023'] > 0].copy()

# ==============================================================================
# 1. FACT RIESGO VIAL PROVINCIA
# ==============================================================================
logger.info("\n1. Generando Fact de Riesgo Vial Provincial...")

# Cálculos de ratios
df_fact['vehiculos_por_1000_hab_2023'] = (df_fact['total_vehiculos'] / df_fact['poblacion_2023']) * 1000

# Se usa total_vehiculos como denominador, protegiendo contra división por 0
df_fact['siniestros_por_10k_vehiculos_mixto'] = np.where(
    df_fact['total_vehiculos'] > 0,
    (df_fact['total_siniestros'] / df_fact['total_vehiculos']) * 10000,
    0
)

df_fact['fallecidos_por_100k_hab'] = (df_fact['total_fallecidos'] / df_fact['poblacion_2023']) * 100000

# Cálculo del Índice de Riesgo Relativo
# Se normalizan siniestros y fallecidos sobre su respectiva media y se le da 
# 60% peso a fallecidos (mayor gravedad) y 40% a siniestros.
mean_fallecidos = df_fact['fallecidos_por_100k_hab'].mean()
mean_siniestros = df_fact['siniestros_por_10k_vehiculos_mixto'].mean()

val_fallecidos = df_fact['fallecidos_por_100k_hab'] / mean_fallecidos if mean_fallecidos > 0 else 0
val_siniestros = df_fact['siniestros_por_10k_vehiculos_mixto'] / mean_siniestros if mean_siniestros > 0 else 0

df_fact['indice_riesgo_relativo'] = (val_fallecidos * 0.6) + (val_siniestros * 0.4)

# Ranking (1 es el índice más alto = Peor Riesgo)
df_fact['ranking_riesgo_relativo'] = df_fact['indice_riesgo_relativo'].rank(ascending=False).astype(int)

# Adv warning textual
df_fact['advertencia_cruce'] = "NOTA: Cálculo mixto cruzando Siniestralidad/Mortalidad (2021) frente a Parque Automotor y Población (2023)."
df_fact['fecha_procesamiento_utc'] = TIMESTAMP_UTC

# Guardar Fact
out_fact = os.path.join(GLD_DIR, "gld_fact_riesgo_vial_provincia")
df_fact.to_parquet(out_fact + ".parquet", index=False)
df_fact.to_csv(out_fact + ".csv", index=False, encoding='utf-8')

# ==============================================================================
# 2. RANKING DE PROVINCIAS POR RIESGO RELATIVO
# ==============================================================================
logger.info("\n2. Generando Ranking de Provincias (Riesgo Relativo)...")
cols_ranking = [
    'ranking_riesgo_relativo',
    'cod_provincia',
    'desc_provincia',
    'indice_riesgo_relativo',
    'fallecidos_por_100k_hab',
    'siniestros_por_10k_vehiculos_mixto',
    'vehiculos_por_1000_hab_2023',
    'fecha_procesamiento_utc'
]
df_ranking = df_fact.sort_values(by='ranking_riesgo_relativo', ascending=True)[cols_ranking]

out_rank = os.path.join(GLD_DIR, "gld_ranking_provincias_riesgo_relativo")
df_ranking.to_parquet(out_rank + ".parquet", index=False)
df_ranking.to_csv(out_rank + ".csv", index=False, encoding='utf-8')
logger.info("Top 3 Riesgo Vial:")
if not df_ranking.empty:
    logger.info(f"\n{df_ranking[['ranking_riesgo_relativo', 'desc_provincia', 'indice_riesgo_relativo']].head(3)}")

# ==============================================================================
# 3. CAUSA PRINCIPAL TOP 1 POR PROVINCIA
# ==============================================================================
logger.info("\n3. Generando Causa Principal Top 1 por Provincia...")
# Agrupar siniestros por provincia y causa
df_causas = df_siniestros.groupby(['cod_provincia', 'desc_provincia', 'causa_siniestro']).size().reset_index(name='conteo_siniestros')

# Ordenar descendentemente por conteo y hacer drop_duplicates sobre provincia para retener el #1
df_causa_top1 = df_causas.sort_values(
    by=['cod_provincia', 'conteo_siniestros'], 
    ascending=[True, False]
).drop_duplicates(subset=['cod_provincia'], keep='first')

df_causa_top1['fecha_procesamiento_utc'] = TIMESTAMP_UTC

out_causa = os.path.join(GLD_DIR, "gld_causa_principal_por_provincia")
df_causa_top1.to_parquet(out_causa + ".parquet", index=False)
df_causa_top1.to_csv(out_causa + ".csv", index=False, encoding='utf-8')

# ==============================================================================
# 4. OUTLIERS: BAJA MOTORIZACIÓN PERO ALTA SEVERIDAD MORTAL
# ==============================================================================
logger.info("\n4. Detectando Outliers (Baja Motorización y Alta Severidad)...")
# MOTORIZACIÓN = vehiculos_por_1000_hab_2023
# SEVERIDAD = fallecidos_por_100k_hab

# Criterio: 
# Baja Motorización = Menos que el percentil 40 (Q40)
# Alta Severidad (Mortalidad) = Mayor al percentil 75 (Q75)
p40_mot = df_fact['vehiculos_por_1000_hab_2023'].quantile(0.40)
p75_sev = df_fact['fallecidos_por_100k_hab'].quantile(0.75)

df_outliers = df_fact[
    (df_fact['vehiculos_por_1000_hab_2023'] < p40_mot) & 
    (df_fact['fallecidos_por_100k_hab'] > p75_sev)
].copy()

# Guardamos el documento del criterio usado
df_outliers['criterio_usado'] = f"Motorización < Q40 ({p40_mot:.1f} veh/1k hab) AND Mortalidad > Q75 ({p75_sev:.1f} fall/100k hab)"

cols_outliers = [
    'cod_provincia', 'desc_provincia', 
    'vehiculos_por_1000_hab_2023', 'fallecidos_por_100k_hab', 
    'criterio_usado', 'fecha_procesamiento_utc'
]
df_outliers = df_outliers[cols_outliers]

out_outliers = os.path.join(GLD_DIR, "gld_outliers_baja_motorizacion_alta_severidad")
df_outliers.to_parquet(out_outliers + ".parquet", index=False)
df_outliers.to_csv(out_outliers + ".csv", index=False, encoding='utf-8')
logger.info("Outliers Encontrados:")
if len(df_outliers) > 0:
    logger.info(f"\n{df_outliers[['desc_provincia', 'vehiculos_por_1000_hab_2023', 'fallecidos_por_100k_hab']]}")
else:
    logger.info("No se encontraron provincias bajo estas estrictas condiciones anómalas.")

logger.info("\n>>> PROCESO CAPA GOLD COMPLETADO CON ÉXITO <<<")
