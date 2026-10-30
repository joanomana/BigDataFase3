# Análisis de los resultados Saber TyT 2019-2 con Apache Spark

**Curso:** Big Data (202016911) – Universidad Nacional Abierta y a Distancia (UNAD)
**Actividad:** Tarea 3 – Procesamiento de Datos con Apache Spark
**Autor:** Joan Sebastian Omaña Suarez

## Problema

¿Cómo varían los resultados de los estudiantes técnicos y tecnológicos de Colombia según el departamento, el tipo de institución y sus condiciones socioeconómicas, y en qué módulos se presentan los puntajes más bajos?

## Conjunto de datos

- **Nombre:** Resultados de las pruebas Saber TyT Genéricas 2019-2
- **Fuente:** ICFES, publicado en el portal de datos abiertos de Colombia ([datos.gov.co](https://www.datos.gov.co))
- **Tamaño:** 76.354 registros y 102 columnas (aprox. 73 MB)
- **Contenido:** datos del estudiante, de su familia y de la institución, y los puntajes de los módulos de razonamiento cuantitativo, lectura crítica, competencias ciudadanas, inglés y comunicación escrita, además del puntaje global.

## Solución

El script `batch_saber_tyt.py` realiza un procesamiento en batch con PySpark:

1. **Carga:** lee el archivo CSV almacenado en HDFS como un DataFrame de Spark.
2. **Limpieza:** selecciona las columnas relevantes, cuenta los valores nulos, convierte los puntajes a tipo numérico, estandariza los textos, reemplaza los vacíos por `SIN INFORMACION` y elimina duplicados.
3. **Transformación:** crea las columnas `NIVEL_DESEMPENO` (bajo, medio o alto según los terciles del puntaje global) y `MODULO_MAS_BAJO` (el módulo con el menor puntaje de cada estudiante).
4. **Análisis exploratorio (EDA):** estadísticas descriptivas; promedios por módulo, departamento, origen de la institución, metodología, estrato, acceso a internet, género y horas de trabajo; correlación de cada módulo con el puntaje global, y una operación con RDD (`map` + `reduceByKey`).
5. **Visualización:** genera cinco gráficas en la carpeta `graficas_tyt` y tablas de resultados en CSV en la carpeta `resultados_tyt`.

## Principales hallazgos

- **Razonamiento cuantitativo** es el módulo con menor promedio (88,15) y el más débil para 24.596 estudiantes.
- Existe una **brecha regional** de casi 30 puntos entre Risaralda (104,01) y Chocó (74,73).
- Los estudiantes con **internet en casa** promedian 97,3 frente a 89,97 de quienes no tienen.
- El puntaje global aumenta del **estrato 1** (90,02) al **estrato 4** (101,45).
- **Lectura crítica** es el módulo con mayor correlación con el puntaje global (r = 0,79).

## Requisitos

- Ubuntu Server 22.04 con Java 11 (OpenJDK)
- Apache Hadoop 3.3.6 (HDFS)
- Apache Spark 3.5.8 y PySpark 3.5.8
- Python 3 con `pandas` y `matplotlib`

## Instrucciones de ejecución

1. Descargue el dataset desde datos.gov.co (*Resultados Saber TyT Genéricas 2019-2*, opción **Exportar → CSV**).

2. Inicie HDFS y cargue el archivo (como usuario `hadoop`):

   ```bash
   start-dfs.sh
   hdfs dfs -mkdir -p /Tarea3
   hdfs dfs -put saber_tyt.csv /Tarea3/
   hdfs dfs -ls /Tarea3
   ```

3. Instale las librerías de Python:

   ```bash
   sudo pip install pyspark==3.5.8 pandas matplotlib
   ```

4. Ejecute el análisis:

   ```bash
   python3 batch_saber_tyt.py
   ```

   Opcionalmente puede indicar otra ruta para el archivo:

   ```bash
   python3 batch_saber_tyt.py hdfs://localhost:9000/ruta/otro_archivo.csv
   ```

5. Revise los resultados en la consola, las gráficas en `~/graficas_tyt` y las tablas en `~/resultados_tyt`.

> **Nota:** en máquinas virtuales con poca memoria (2 GB) se recomienda activar un archivo swap y detener YARN (`stop-yarn.sh`), ya que el script se ejecuta en modo local y solo necesita HDFS.

## Estructura del repositorio

```
├── README.md
├── batch_saber_tyt.py
└── graficas_tyt/
    ├── 01_promedio_por_modulo.png
    ├── 02_promedio_por_departamento.png
    ├── 03_promedio_por_origen.png
    ├── 04_promedio_por_estrato.png
    └── 05_histograma_puntaje_global.png
```
