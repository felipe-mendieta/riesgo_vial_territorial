import os
import sys
import pandas as pd
from datetime import datetime, timezone
from utils import get_logger, safe_read_parquet

logger = get_logger("build_silver_integrated")

# ==============================================================================
# CONFIGURACIÓN DE RUTAS
# ==============================================================================
STD_DIR = r"..\silver\standardized"
INT_DIR = r"..\silver\integrated"

# Asegurar existencia del directorio de salida
os.makedirs(INT_DIR, exist_ok=True)

TIMESTAMP_UTC = datetime.now(timezone.utc).isoformat()

logger.info(">>> INICIANDO CONSTRUCCIÓN DE CAPA SILVER INTEGRATED <<<")

# ==============================================================================
# 1. PARQUE POR PROVINCIA 2023
# ==============================================================================
# Representa la cantidad total agregada de vehículos matriculados en cada provincia
# para el año de referencia 2023.
logger.info("\n1. Procesando Parque Vehicular por Provincia...")
veh_file = os.path.join(STD_DIR, "std_vehiculos_matriculados_registro_2023.parquet")
df_veh = safe_read_parquet(veh_file, logger)

if df_veh is not None:
    # Agrupar por provincia y contar registros
    df_parque = df_veh.groupby('cod_provincia').size().reset_index(name='total_vehiculos')
    df_parque['anio_parque'] = 2023
    df_parque['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    # Guardar en integrated
    out_parque = os.path.join(INT_DIR, "int_parque_por_provincia_2023.parquet")
    out_parque_csv = os.path.join(INT_DIR, "int_parque_por_provincia_2023.csv")
    df_parque.to_parquet(out_parque, index=False)
    df_parque.to_csv(out_parque_csv, index=False, encoding='utf-8')
    logger.info("Ejemplo de Parque por Provincia:")
    logger.info(f"\n{df_parque.head(3)}")
else:
    logger.warning("Falta parque vehicular. Generando DataFrame vacío para Cobertura.")
    df_parque = pd.DataFrame(columns=['cod_provincia', 'total_vehiculos', 'anio_parque', 'fecha_procesamiento_utc'])

# ==============================================================================
# 2. SINIESTRALIDAD POR PROVINCIA 2021
# ==============================================================================
# Refleja el consolidado de siniestros, víctimas, lesionados y fallecidos a 
# nivel provincial ocurridos durante el año 2021.
logger.info("\n2. Procesando Siniestralidad por Provincia...")
sin_file = os.path.join(STD_DIR, "std_siniestros_evento_2021.parquet")
df_sin = safe_read_parquet(sin_file, logger)

if df_sin is not None:
    df_siniestralidad = df_sin.groupby('cod_provincia').agg(
        total_siniestros=('cod_provincia', 'count'),
        total_fallecidos=('fallecidos', 'sum'),
        total_lesionados=('lesionados', 'sum'),
        total_victimas=('total_victimas', 'sum')
    ).reset_index()
    
    df_siniestralidad['anio_siniestros'] = 2021
    df_siniestralidad['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    out_siniestralidad = os.path.join(INT_DIR, "int_siniestralidad_por_provincia_2021.parquet")
    out_siniestralidad_csv = os.path.join(INT_DIR, "int_siniestralidad_por_provincia_2021.csv")
    df_siniestralidad.to_parquet(out_siniestralidad, index=False)
    df_siniestralidad.to_csv(out_siniestralidad_csv, index=False, encoding='utf-8')
    logger.info("Ejemplo de Siniestralidad por Provincia:")
    logger.info(f"\n{df_siniestralidad.head(3)}")
else:
    logger.warning("Faltan siniestros. Generando DataFrame vacío para Cobertura.")
    df_siniestralidad = pd.DataFrame(columns=['cod_provincia', 'total_siniestros', 'total_fallecidos', 'total_lesionados', 'total_victimas', 'anio_siniestros', 'fecha_procesamiento_utc'])

# ==============================================================================
# 3. POBLACIÓN
# ==============================================================================
# Contiene los datos demográficos extraídos de las proyecciones provinciales.
# Actúa como una dimensión factible de cruces transversales.
logger.info("\n3. Procesando Población Demográfica...")
pob_file = os.path.join(STD_DIR, "std_poblacion_provincias.parquet")
df_pob = safe_read_parquet(pob_file, logger)

if df_pob is not None:
    # Reutilizar tal cual estandarizado, actualizando timestamp
    df_pob_int = df_pob.copy()
    df_pob_int['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    out_pob = os.path.join(INT_DIR, "int_poblacion_demografia_provincia.parquet")
    out_pob_csv = os.path.join(INT_DIR, "int_poblacion_demografia_provincia.csv")
    df_pob_int.to_parquet(out_pob, index=False)
    df_pob_int.to_csv(out_pob_csv, index=False, encoding='utf-8')
    logger.info("Ejemplo de Población Demográfica:")
    logger.info(f"\n{df_pob_int.head(3)}")
else:
    logger.warning("Falta población. Generando DataFrame vacío para Cobertura.")
    df_pob_int = pd.DataFrame(columns=['cod_provincia', 'poblacion_2023', 'poblacion_2021'])

# ==============================================================================
# 4. COBERTURA (LLAVE PROVINCIA MATRIZ)
# ==============================================================================
# Esta tabla utiliza el catálogo geográfico DPA como eje vertebral (espina dorsal).
# Su propósito es asegurar que todas las provincias existan y validar si tienen
# información disponible en cada uno de los dominios (parque, siniestros y población)
# a través de un simple Left Join y el levantamiento de banderas lógicas (flags).
logger.info("\n4. Procesando Cobertura y Banderas...")
dpa_file = os.path.join(STD_DIR, "std_dim_provincia_dpa.parquet")
df_cobertura = safe_read_parquet(dpa_file, logger) # 24 registros + Zona En Estudio (90) si la hay

if df_cobertura is None:
    logger.error("Dimensión DPA no encontrada. CRÍTICO. Abortando integración.")
    sys.exit(1)

# Uniones con Left Join
df_cobertura = df_cobertura.merge(df_parque[['cod_provincia', 'total_vehiculos']], on='cod_provincia', how='left')
df_cobertura = df_cobertura.merge(df_siniestralidad[['cod_provincia', 'total_siniestros']], on='cod_provincia', how='left')
df_cobertura = df_cobertura.merge(df_pob_int[['cod_provincia', 'poblacion_2023']], on='cod_provincia', how='left')

# Generar Flags booleanos si el dato no es nulo
df_cobertura['tiene_parque'] = df_cobertura['total_vehiculos'].notna()
df_cobertura['tiene_siniestros'] = df_cobertura['total_siniestros'].notna()
df_cobertura['tiene_poblacion'] = df_cobertura['poblacion_2023'].notna()

# Rellenar con ceros las proyecciones agrupadas que no existían, si es necesario, 
# pero la cobertura en sí lo importante son las flags que pide el usuario.
df_cobertura['total_vehiculos'] = df_cobertura['total_vehiculos'].fillna(0).astype(int)
df_cobertura['total_siniestros'] = df_cobertura['total_siniestros'].fillna(0).astype(int)
df_cobertura['poblacion_2023'] = df_cobertura['poblacion_2023'].fillna(0).astype(int)

df_cobertura['fecha_procesamiento_utc'] = TIMESTAMP_UTC

out_cobertura = os.path.join(INT_DIR, "int_cobertura_llave_provincia.parquet")
out_cobertura_csv = os.path.join(INT_DIR, "int_cobertura_llave_provincia.csv")
df_cobertura.to_parquet(out_cobertura, index=False)
df_cobertura.to_csv(out_cobertura_csv, index=False, encoding='utf-8')
logger.info("Ejemplo de Cobertura Multidimensional:")
logger.info(f"\n{df_cobertura[['cod_provincia', 'desc_provincia', 'tiene_parque', 'tiene_siniestros', 'tiene_poblacion']].head(5)}")

logger.info("\n>>> PROCESO DE INTEGRACIÓN COMPLETADO CON ÉXITO <<<")
