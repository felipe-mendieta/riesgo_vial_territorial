# Dashboard Gerencial: Análisis de Riesgo Vial Territorial

Este documento presenta la estructura y el contenido de un dashboard gerencial enfocado en la toma de decisiones para mitigar el riesgo vial a nivel nacional, basado en datos de la capa Gold procesada (Siniestralidad 2021, Población y Parque Automotor 2023).

---

## 📌 1. Resumen Ejecutivo (KPIs Principales)

Estos indicadores deben estar en la parte superior del dashboard, mostrando cifras globales y permitiendo filtros por región o provincia.

*   **Índice de Riesgo Relativo Promedio (IRR):** Un valor de 1.0 representa el promedio nacional. Valores superiores a 1 indican mayor peligro.
*   **Tasa de Mortalidad Global:** Fallecidos totales por cada 100,000 habitantes en el país.
*   **Tasa de Siniestralidad Global:** Siniestros por cada 10,000 vehículos.
*   **Total de Víctimas (Lesionados + Fallecidos):** Magnitud absoluta del impacto social y en salud pública.

---

## 📊 2. Visualizaciones y Análisis

### A. Ranking de Provincias Críticas (Top Riesgo Relativo)
*   **Tipo de Gráfica:** Gráfico de Barras Horizontales (ordenado de mayor a menor IRR). Resaltar en color rojo las barras con IRR > 1.2.
*   **Qué representa:** Identifica las provincias donde el riesgo de sufrir un siniestro vial o fallecer es desproporcionadamente alto, equilibrando el tamaño de su población y su parque automotor.
*   **Insights Clave:** El "Top 3" de riesgo está liderado por **Morona Santiago** (IRR 2.1), **Santo Domingo de los Tsáchilas** (IRR 1.62) y **Santa Elena** (IRR 1.39). Guayas y Chimborazo también están en zona roja operativa.
*   **Decisiones/Acciones Recomendadas:** 
    *   **Priorización de Presupuesto:** Redirigir fondos de seguridad vial, campañas y fiscalización de inmediato hacia estas 5 provincias críticas.
    *   **Auditorías Viales:** Enviar equipos de ingeniería vial urgente a Santo Domingo y Morona Santiago para auditar el estado de las vías principales.

### B. Matriz de Dispersión: Motorización vs. Severidad (Detección de Anomalías)
*   **Tipo de Gráfica:** Gráfico de Dispersión (Scatter Plot).
    *   Eje X: Tasa de Motorización (Vehículos por 1k habitantes).
    *   Eje Y: Tasa de Mortalidad (Fallecidos por 100k habitantes).
    *   Color: Resaltar puntos atípicos (Outliers) en naranja o rojo.
*   **Qué representa:** Compara qué tan saturada de vehículos está una provincia frente a qué tan letales son los siniestros que ocurren en ella. 
*   **Insights Clave:** Existe una gran anomalía (Outlier): **Morona Santiago**. Tiene una motorización muy baja (87 veh / 1k hab, frente a provincias con más de 200), pero tiene la mortalidad más alta del país (35 fallecidos / 100k hab). 
*   **Decisiones/Acciones Recomendadas:**
    *   **Investigación en Sitio:** Dado que tienen pocos vehículos matriculados, el problema podría deberse a tráfico "de paso" (vehículos pesados cruzando la provincia), vías peligrosas (barrancos, mal clima) o ausencia temporal de atención médica rápida posterior al accidente.
    *   **Aumentar Control en Carreteras:** Implementar controles estáticos y móviles en las rutas que cruzan Morona Santiago.

### C. Cuadrantes de Riesgo: Siniestralidad vs. Mortalidad
*   **Tipo de Gráfica:** Matriz de 4 Cuadrantes. 
    *   Líneas divisorias: Promedio nacional de Siniestros y Promedio de Mortalidad.
*   **Qué representa:** Segmenta a las provincias en estrategias de intervención. Por ejemplo, una alta tasa de siniestros pero baja mortalidad indica choques leves (tráfico denso/urbano). Alta mortalidad y baja siniestralidad indica choques a alta velocidad o en carreteras.
*   **Insights Clave:** 
    *   **Guayas y Santa Elena:** Altísima tasa de siniestralidad de vehículos y lesionados, pero una mortalidad moderada-baja. Son zonas de accidentes urbanos masivos.
    *   **Bolívar y Orellana:** Menores siniestros, pero cuando suceden, son letales.
*   **Decisiones/Acciones Recomendadas:**
    *   **Cuadrante Urbano (Guayas, Pichincha):** Ordenar el tráfico en ciudades, fiscalización de motos, semaforización y control de intersecciones.
    *   **Cuadrante Letal (Bolívar, Morona):** Controles de velocidad en rutas interprovinciales, y mejora en los tiempos de respuesta de ambulancias en zonas rurales.

### D. Distribución de la Causa Principal
*   **Tipo de Gráfica:** Gráfico de Donas/Anillo por región o mapa coroplético (Heatmap) filtrado por causa.
*   **Qué representa:** Muestra la razón principal detrás de la accidentalidad en cada provincia para poder hacer campañas dirigidas.
*   **Insights Clave:** La **"Impericia e Imprudencia del Conductor"** es la causa número uno, dominando abrumadoramente en 23 de las 24 provincias. La única excepción es la provincia de **Carchi**, cuya causa principal reportada es el **"Exceso de Velocidad"**.
*   **Decisiones/Acciones Recomendadas:**
    *   **Acción Nacional:** El problema de Ecuador es el factor humano. Modificar la malla curricular para la obtención y renovación de licencias a nivel nacional, haciendo pruebas prácticas mucho más estrictas.
    *   **Acción Local (Carchi):** Instalación inmediata de fotorradares, reductores de velocidad físicos (rompevelocidades) y operativos de la patrulla caminera con radares móviles en los puntos negros de Carchi.

---

## 🎯 3. Estructura y Navegabilidad Sugerida para el Dashboard (UX/UI)

1.  **Filtros Globales (Parte Superior):** Provincia, Rango de Índices de Riesgo.
2.  **Banda de KPIs (Resumen Ejecutivo):** Tarjetas grandes con métricas clave y variaciones.
3.  **Sección Central Izquierda (Análisis Estratégico):** Ranking General y Matriz de Dispersión (para ver el panorama general y atípicos).
4.  **Sección Central Derecha (Análisis Operativo):** Matriz de Siniestralidad vs Mortalidad.
5.  **Sección Inferior (Causalidad):** Detalle de causas principales y botón para "Exportar Reporte" para entes de fiscalización.

*Documento generado automatizadamente en base al procesamiento Gold de siniestralidad integral.*
