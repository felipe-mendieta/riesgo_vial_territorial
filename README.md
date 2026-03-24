# 🚗 Riesgo Vial Territorial

Un producto de datos analítico para comprender el problema de la siniestralidad vial en las provincias del Ecuador de manera integral, justa y proporcional.

## 🎯 Contexto y Problemática

Desde la gestión pública y la planificación territorial, uno de los desafíos más importantes es comprender el problema de la siniestralidad vial más allá de los números absolutos. Con frecuencia, las decisiones se toman observando únicamente cuántos siniestros, fallecidos o lesionados registra una provincia. 

Sin embargo, ese enfoque puede ser engañoso: no distingue entre territorios grandes y pequeños, ni considera el tamaño del parque vehicular o la población expuesta. Por esta razón, se requiere construir un producto de datos que integre distintas fuentes oficiales en una sola vista analítica para **comparar, priorizar territorios, focalizar intervenciones y mejorar la toma de decisiones**.

## 📊 Fuentes de Datos

El proyecto se basa en datos oficiales del Instituto Nacional de Estadística y Censos (INEC):

1. **Vehículos Matriculados**: Aproximación al tamaño del parque vehicular.
2. **Siniestros de Tránsito**: Expresión del problema vial observado.
3. **Proyecciones de Población**: Contexto demográfico del territorio.
4. **Clasificador Geográfico (DPA)**: Dimensión geográfica para homologación y cruce territorial.

*Para un mayor detalle, consultar la carpeta `raw/` y la documentación de las fuentes.*

## 🏗️ Arquitectura de Datos (Pipeline ETL)

El proyecto utiliza una arquitectura de datos en capas estructurada e interconectada procesada mediante scripts de Python:

- 🥉 **Capa Bronze**: Extracción de datos crudos (`raw/`) y conversión a formatos estandarizados (CSV/Parquet).
- 🥈 **Capa Silver**: 
  - *Standardized*: Limpieza, estandarización de columnas, parseo de tipos de datos y limpieza de textos.
  - *Integrated*: Agrupaciones clave, transformación a tablas de hechos, y cruces con dimensiones transversales (como parámetros geográficos).
- 🥇 **Capa Gold**: Generación de métricas de valor final para el negocio, consolidación del Data Mart, cálculo de rankings relativos, detección de valores atípicos (outliers) y tablas modeladas.

## 🚀 Cómo Ejecutar el Pipeline

El pipeline completo está orquestado desde un script central. Asegúrate de tener las dependencias de Python instaladas (como `pandas`, `pyarrow`, etc.). Para ejecutar el proceso ETL desde Bronze hasta Gold, ejecuta desde la raíz del proyecto:

```bash
python scripts/run_all_pipeline.py
```

Esto ejecutará secuencialmente cada uno de los trabajos de procesamiento de la capa Bronze, la limpieza en Silver, y la agregación en Gold mostrando los logs de estado por consola.

## 💡 Preguntas Clave que responde el Producto

Con este modelo se busca servir a un director de planificación, movilidad o analítica para responder las siguientes interrogantes estratégicas:

- ¿Qué provincias del Ecuador presentan un riesgo vial relativamente **más alto** cuando consideramos su proporción de vehículos, siniestros registrados y población?
- ¿Qué provincias deberían **priorizarse** para acciones de prevención?
- ¿Dónde el problema vial parece más intenso vs. dónde hay más severidad?
- ¿Qué territorios combinan alta severidad y condiciones estructurales desfavorables?
- ¿Qué causas concentran la mayor parte de los siniestros de siniestros dependiendo de la provincia?
- ¿Dónde los volúmenes totales engañan y **las tasas relativas cambian la historia**?

## 📈 Métricas y KPIs Generados

El modelo final estructura la información de manera que cada fila represente a una provincia, integrando y permitiendo el cálculo de métricas como:
- 🚗 Vehículos por cada 1.000 habitantes.
- 💥 Siniestros por cada 10.000 vehículos.
- ⚰️ Fallecidos por cada 100.000 habitantes.
- 🏆 Ranking relativo de riesgo vial por provincia.
- 🔍 Identificación de provincias atípicas (ej. zonas con baja motorización pero alta proporción de severidad en accidentes).
