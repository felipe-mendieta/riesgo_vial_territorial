import os
import sys
import logging
import pandas as pd

def get_logger(name):
    """
    Configura y retorna un logger estandarizado para consola.
    """
    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        # Formato: [2023-10-31 10:00:00] [INFO] [script_name] - Mensaje
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [%(name)s] - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
        
        ch = logging.StreamHandler(sys.stdout)
        ch.setFormatter(formatter)
        logger.addHandler(ch)
        
    return logger

def safe_read_csv(filepath, sep=";", decimal=".", low_memory=False, logger=None):
    """
    Lee un archivo CSV implementando manejo de excepciones y fallback de codificación.
    Retorna el DataFrame o None si falla completamente.
    """
    _logger = logger or logging.getLogger(__name__)
    
    if not os.path.exists(filepath):
        _logger.error(f"Error crítico: Archivo no encontrado - {filepath}")
        return None
        
    try:
        df = pd.read_csv(filepath, sep=sep, encoding="utf-8", decimal=decimal, low_memory=low_memory)
        _logger.info(f"Lectura exitosa (UTF-8): {filepath}")
        return df
    except UnicodeDecodeError:
        _logger.warning(f"UTF-8 falló para {filepath}. Intentando con latin-1...")
        try:
            df = pd.read_csv(filepath, sep=sep, encoding="latin-1", decimal=decimal, low_memory=low_memory)
            _logger.info(f"Lectura exitosa (latin-1): {filepath}")
            return df
        except Exception as e:
            _logger.error(f"Falla fatal al leer CSV con latin-1: {e}")
            return None
    except Exception as e:
        _logger.error(f"No se pudo leer el CSV {filepath}: {e}")
        return None

def safe_read_excel(filepath, sheet_name=0, skiprows=0, dtype=None, logger=None):
    """
    Lee un Excel con manejo de errores o archivo inexistente.
    Retorna el ExcelFile, y para su posterior parseo, si falla, None.
    Si sheet_name es None, se lee el objeto ExcelFile para manejar las hojas.
    """
    _logger = logger or logging.getLogger(__name__)

    if not os.path.exists(filepath):
        _logger.error(f"Error crítico: Archivo Excel no encontrado - {filepath}")
        return None

    try:
        if sheet_name is None:
            # Retornar objeto ExcelFile para iterar hojas
            xl = pd.ExcelFile(filepath)
            _logger.info(f"Lectura exitosa del ExcelFile: {filepath}")
            return xl
        else:
            df = pd.read_excel(filepath, sheet_name=sheet_name, skiprows=skiprows, dtype=dtype)
            _logger.info(f"Lectura exitosa de hoja '{sheet_name}' en {filepath}")
            return df
    except Exception as e:
        _logger.error(f"Falla al leer Excel {filepath}: {e}")
        return None

def safe_read_parquet(filepath, logger=None):
    """
    Lee un Parquet con manejo seguro de excepciones e inexistencia.
    """
    _logger = logger or logging.getLogger(__name__)
    
    if not os.path.exists(filepath):
        _logger.warning(f"Archivo Parquet faltante/no encontrado: {filepath}")
        return None
        
    try:
        df = pd.read_parquet(filepath)
        _logger.info(f"Lectura exitosa de Parquet: {filepath}")
        return df
    except Exception as e:
        _logger.error(f"No se pudo leer el Parquet {filepath}: {e}")
        return None
