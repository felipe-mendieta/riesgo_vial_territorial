import pandas as pd
import os
import sys
from datetime import datetime, timezone
import unicodedata
from utils import get_logger, safe_read_parquet

logger = get_logger("generar_silver_standardized")

def clean_text(series):
    """
    Convierte a mayúsculas, elimina espacios en blanco extras,
    elimina tildes/acentos diacríticos, y reemplaza campos vacíos.
    """
    # Rellenar nulos temporalmente
    s = series.fillna("NO DEFINIDO")
    # A mayúsculas y sin espacios a los extremos
    s = s.astype(str).str.upper().str.strip()
    
    # Remover tildes
    s = s.apply(lambda x: ''.join(c for c in unicodedata.normalize('NFKD', x) if not unicodedata.combining(c)))
    
    # Rellenar strings que hayan quedado vacíos
    s = s.replace("", "NO DEFINIDO")
    s = s.replace("NAN", "NO DEFINIDO")
    
    return s

# Directorios de entrada y salida
BRONZE_DIR = r"..\bronze"
SILVER_DIR = r"..\silver\standardized"

os.makedirs(SILVER_DIR, exist_ok=True)

TIMESTAMP_UTC = datetime.now(timezone.utc).isoformat()

# ==============================================================================
# 1. Dimensión DPA (Provincias)
# ==============================================================================
input_dpa = os.path.join(BRONZE_DIR, "brz_dim_provincia_dpa_2024.parquet")
logger.info(f"-> Estandarizando {input_dpa}")
df_dpa = safe_read_parquet(input_dpa, logger)

if df_dpa is None:
    logger.error("DPA file is missing. Critical dependency. Aborting.")
    sys.exit(1)


df_dpa = df_dpa.rename(columns={
    'DPA_PROVIN': 'cod_provincia',
    'DPA_DESPRO': 'desc_provincia'
})
# Formatear el código a 2 dígitos pad ceros (e.g. 01, 09, 24)
df_dpa['cod_provincia'] = df_dpa['cod_provincia'].astype(str).str.zfill(2)
df_dpa['desc_provincia'] = clean_text(df_dpa['desc_provincia'])
df_dpa['fecha_procesamiento_utc'] = TIMESTAMP_UTC

# Guardar mapeo en memoria para cruces
dpa_map = dict(zip(df_dpa['desc_provincia'], df_dpa['cod_provincia']))

dpa_csv = os.path.join(SILVER_DIR, "std_dim_provincia_dpa.csv")
dpa_parquet = os.path.join(SILVER_DIR, "std_dim_provincia_dpa.parquet")
df_dpa.to_csv(dpa_csv, index=False, encoding="utf-8")
df_dpa.to_parquet(dpa_parquet, index=False)
logger.info(f"   Dimensión guardada: {len(df_dpa)} registros.")

# ==============================================================================
# 2. Población Provincias
# ==============================================================================
input_pob = os.path.join(BRONZE_DIR, "brz_poblacion_provincias.parquet")
logger.info(f"\n-> Estandarizando {input_pob}")
df_pob = safe_read_parquet(input_pob, logger)

if df_pob is None:
    logger.warning("Saltando Población. Archivo no encontrado.")
else:

    # Renombrar e identificar la columna clave textual
    df_pob['provincia'] = clean_text(df_pob['provincia'])
    # Hay que tener cuidado de Galápagos vs Galapagos, etc. clean_text solucionó las tildes.
    df_pob['cod_provincia'] = df_pob['provincia'].map(dpa_map)
    
    # Mantener solo columnas solicitadas y en snake_case
    df_pob = df_pob[['cod_provincia', 'provincia', 'hoja_origen', 'poblacion_2023', 'poblacion_2021']]
    
    # Casteo numérico (Forzamos Int32 de pandas que soporta nulos por seguridad, aunque sabemos que no faltan)
    df_pob['poblacion_2023'] = df_pob['poblacion_2023'].astype("Int32")
    df_pob['poblacion_2021'] = df_pob['poblacion_2021'].astype("Int32")
    df_pob['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    pob_csv = os.path.join(SILVER_DIR, "std_poblacion_provincias.csv")
    pob_parquet = os.path.join(SILVER_DIR, "std_poblacion_provincias.parquet")
    df_pob.to_csv(pob_csv, index=False, encoding="utf-8")
    df_pob.to_parquet(pob_parquet, index=False)
    logger.info(f"   Población guardada. Nulos en cod_provincia: {df_pob['cod_provincia'].isna().sum()}")

# ==============================================================================
# 3. Siniestros de Tránsito
# ==============================================================================
input_sin = os.path.join(BRONZE_DIR, "brz_siniestros_transito_2021.parquet")
logger.info(f"\n-> Estandarizando {input_sin}")
df_sin = safe_read_parquet(input_sin, logger)

if df_sin is None:
    logger.warning("Saltando Siniestros. Archivo no encontrado.")
else:

    dict_meses = {
        'ENERO': 1, 'FEBRERO': 2, 'MARZO': 3, 'ABRIL': 4,
        'MAYO': 5, 'JUNIO': 6, 'JULIO': 7, 'AGOSTO': 8,
        'SEPTIEMBRE': 9, 'OCTUBRE': 10, 'NOVIEMBRE': 11, 'DICIEMBRE': 12
    }
    
    # Limpiar todas las cadenas de texto
    for col in ['MES', 'DIA', 'PROVINCIA', 'CANTÓN', 'ZONA', 'CLASE', 'CAUSA']:
        df_sin[col] = clean_text(df_sin[col])
    
    # Rango horario lo mantenemos, pues no es necesario convertir a fecha completa sino a su propio campo categórico
    df_sin['HORA'] = df_sin['HORA'].fillna('NO DEFINIDO').astype(str).str.strip()
    
    # Enriquecimiento y cruces
    df_sin['mes_siniestro'] = df_sin['MES'].map(dict_meses).astype("Int32")
    df_sin['cod_provincia'] = df_sin['PROVINCIA'].map(dpa_map)
    
    df_sin = df_sin.rename(columns={
        'ANIO': 'anio_siniestro',
        'DIA': 'dia_semana_siniestro',
        'HORA': 'rango_hora_siniestro',
        'PROVINCIA': 'desc_provincia',
        'CANTÓN': 'desc_canton',
        'ZONA': 'zona',
        'CLASE': 'clase_siniestro',
        'CAUSA': 'causa_siniestro',
        'NUM_FALLECIDO': 'fallecidos',
        'NUM_LESIONADO': 'lesionados',
        'TOTAL_VICTIMAS': 'total_victimas'
    })
    
    # Reordenar y cast numérico
    for col in ['anio_siniestro', 'fallecidos', 'lesionados', 'total_victimas']:
        df_sin[col] = df_sin[col].fillna(0).astype("Int32") # Asumimos 0 si venían vacías
    
    cols_sin_orden = [
        'cod_provincia', 'desc_provincia', 'desc_canton', 'zona', 
        'anio_siniestro', 'mes_siniestro', 'dia_semana_siniestro', 'rango_hora_siniestro',
        'clase_siniestro', 'causa_siniestro', 
        'fallecidos', 'lesionados', 'total_victimas'
    ]
    df_sin = df_sin[cols_sin_orden].copy()
    df_sin['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    sin_csv = os.path.join(SILVER_DIR, "std_siniestros_evento_2021.csv")
    sin_parquet = os.path.join(SILVER_DIR, "std_siniestros_evento_2021.parquet")
    df_sin.to_csv(sin_csv, index=False, encoding="utf-8")
    df_sin.to_parquet(sin_parquet, index=False)
    logger.info(f"   Siniestros guardados: {len(df_sin)} registros. Nulos en cod_provincia: {df_sin['cod_provincia'].isna().sum()}")

# ==============================================================================
# 4. Vehículos Matriculados
# ==============================================================================
input_veh = os.path.join(BRONZE_DIR, "brz_vehiculos_matriculados_2023.parquet")
logger.info(f"\n-> Estandarizando {input_veh}")
df_veh = safe_read_parquet(input_veh, logger)

if df_veh is None:
    logger.warning("Saltando Vehículos. Archivo no encontrado.")
else:

    df_veh = df_veh.rename(columns={
        'PROVINCIA': 'cod_provincia',
        'CANTÓN': 'cod_canton',
        'MARCA': 'marca',
        'CLASE': 'clase_vehiculo',
        'PASAJEROS': 'pasajeros',
        'TONELAJE': 'tonelaje',
        'COMBUSTIBLE': 'tipo_combustible',
        'MODELO': 'anio_modelo',
        'SERVICIO': 'tipo_servicio',
        'ESTRATONE': 'estrato_tonelaje',
        'ESTRAPASAJERO': 'estrato_pasajero',
        'MES': 'mes_matricula'
    })
    
    # Estandarización de llaves territoriales numéricas
    df_veh['cod_provincia'] = df_veh['cod_provincia'].fillna(99).astype(int).astype(str).str.zfill(2)
    df_veh['cod_canton'] = df_veh['cod_canton'].fillna(9999).astype(int).astype(str).str.zfill(4)
    
    # La marca es el único campo textual en este dataset
    df_veh['marca'] = clean_text(df_veh['marca'])
    
    # Cast de enteros, si no viene valor poner sentinela para no perder tipo Int
    # 'anio_modelo' recibe 9999 temporalmente si es nulo. Pasajeros 0.  
    df_veh['anio_modelo'] = df_veh['anio_modelo'].fillna(9999).astype("Int32")
    df_veh['pasajeros'] = df_veh['pasajeros'].fillna(0).astype("Int32")
    
    # Tonelaje es decimal
    df_veh['tonelaje'] = df_veh['tonelaje'].fillna(0.0).astype("Float32")
    
    # Otras variables son categóricas numéricas 
    for col in ['clase_vehiculo', 'tipo_combustible', 'tipo_servicio', 'estrato_tonelaje', 'estrato_pasajero', 'mes_matricula']:
        df_veh[col] = df_veh[col].fillna(99).astype("Int32")
    
    df_veh['anio_registro'] = 2023
    df_veh['anio_registro'] = df_veh['anio_registro'].astype("Int32")
    
    df_veh['fecha_procesamiento_utc'] = TIMESTAMP_UTC
    
    # Re-ordenamos para dejar las llaves primero
    cols_veh_orden = [
        'cod_provincia', 'cod_canton', 'anio_registro', 'mes_matricula',
        'marca', 'anio_modelo', 'clase_vehiculo', 'tipo_combustible', 'tipo_servicio',
        'pasajeros', 'tonelaje', 'estrato_tonelaje', 'estrato_pasajero', 'fecha_procesamiento_utc'
    ]
    df_veh = df_veh[cols_veh_orden].copy()
    
    veh_csv = os.path.join(SILVER_DIR, "std_vehiculos_matriculados_registro_2023.csv")
    veh_parquet = os.path.join(SILVER_DIR, "std_vehiculos_matriculados_registro_2023.parquet")
    df_veh.to_csv(veh_csv, index=False, encoding="utf-8")
    df_veh.to_parquet(veh_parquet, index=False)
    logger.info(f"   Vehículos guardados: {len(df_veh)} registros.")

logger.info("\n>>> PROCESO DE ESTANDARIZACIÓN COMPLETADO CON ÉXITO <<<")
