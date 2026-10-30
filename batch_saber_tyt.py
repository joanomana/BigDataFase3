# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD, 202016911)
Procesamiento en batch con Apache Spark (PySpark)

Dataset: Resultados de las pruebas Saber TyT Genericas 2019-2 (ICFES - datos.gov.co)
Problema: ¿Como varian los resultados de los estudiantes tecnicos y tecnologicos
segun el departamento, el tipo de institucion, el estrato y otros factores,
y en que modulos se presentan los puntajes mas bajos?


"""
import os
import sys
import matplotlib
matplotlib.use("Agg")  
import matplotlib.pyplot as plt
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DoubleType

# ----------------------------------------------------------------------
# Configuracion
# ----------------------------------------------------------------------
RUTA = sys.argv[1] if len(sys.argv) > 1 else "hdfs://localhost:9000/Tarea3/saber_tyt.csv"
CARPETA_GRAFICAS = os.path.expanduser("~/graficas_tyt")
CARPETA_RESULTADOS = os.path.expanduser("~/resultados_tyt")
os.makedirs(CARPETA_GRAFICAS, exist_ok=True)
os.makedirs(CARPETA_RESULTADOS, exist_ok=True)

MODULOS = {
    "MOD_RAZONA_CUANTITAT_PUNT": "Razonamiento cuantitativo",
    "MOD_LECTURA_CRITICA_PUNT": "Lectura critica",
    "MOD_COMPETEN_CIUDADA_PUNT": "Competencias ciudadanas",
    "MOD_INGLES_PUNT": "Ingles",
    "MOD_COMUNI_ESCRITA_PUNT": "Comunicacion escrita",
}
NUMERICAS = list(MODULOS.keys()) + ["PUNT_GLOBAL"]
CATEGORICAS = [
    "ESTU_GENERO", "ESTU_INST_DEPARTAMENTO", "INST_ORIGEN", "INST_CARACTER_ACADEMICO",
    "ESTU_METODO_PRGM", "ESTU_NIVEL_PRGM_ACADEMICO", "FAMI_ESTRATOVIVIENDA",
    "FAMI_TIENEINTERNET", "FAMI_TIENECOMPUTADOR", "ESTU_HORASSEMANATRABAJA",
]


def titulo(texto):
    print("\n" + "=" * 72)
    print(texto)
    print("=" * 72)


def grafica_barras(pdf, x, y, archivo, titulo_grafica, horizontal=False, color="#2e75b6"):
    """Genera una grafica de barras a partir de un DataFrame de pandas pequeno."""
    alto = max(5, 0.32 * len(pdf)) if horizontal else 6
    plt.figure(figsize=(10, alto))
    if horizontal:
        plt.barh(pdf[x].astype(str), pdf[y], color=color)
        plt.gca().invert_yaxis()
        plt.xlabel(y)
    else:
        plt.bar(pdf[x].astype(str), pdf[y], color=color)
        plt.xticks(rotation=30, ha="right")
        plt.ylabel(y)
    plt.title(titulo_grafica)
    plt.tight_layout()
    plt.savefig(os.path.join(CARPETA_GRAFICAS, archivo), dpi=120)
    plt.close()


# ----------------------------------------------------------------------
# 1. Carga del dataset desde HDFS
# ----------------------------------------------------------------------
spark = (SparkSession.builder
    .appName("Tarea3_SaberTyT_Batch")
    .master("local[2]")                            
    .config("spark.driver.memory", "1g")
    .config("spark.driver.host", "127.0.0.1")      
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.network.timeout", "600s")        
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.shuffle.partitions", "8")   
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate())
spark.sparkContext.setLogLevel("WARN")

titulo("1. CARGA DEL DATASET DESDE HDFS")
df_raw = (spark.read
    .option("header", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .csv(RUTA))
print(f"Ruta: {RUTA}")
print(f"Registros cargados: {df_raw.count()}  |  Columnas: {len(df_raw.columns)}")
df_raw.select("PERIODO", "ESTU_GENERO", "ESTU_INST_DEPARTAMENTO",
        "INST_ORIGEN", "PUNT_GLOBAL").show(5, truncate=False)

# ----------------------------------------------------------------------
# 2. Limpieza y transformacion
# ----------------------------------------------------------------------
titulo("2. LIMPIEZA Y TRANSFORMACION")
df = df_raw.select(["ESTU_CONSECUTIVO"] + CATEGORICAS + NUMERICAS)

print("Valores nulos por columna ANTES de la limpieza:")
df.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in df.columns]) \
.show(vertical=True)

# Puntajes a numero (si traen coma decimal se reemplaza por punto)
for c in NUMERICAS:
    df = df.withColumn(c, F.regexp_replace(F.col(c), ",", ".").cast(DoubleType()))

# Texto en mayusculas y sin espacios; vacios -> "SIN INFORMACION"
for c in CATEGORICAS:
    limpio = F.upper(F.trim(F.col(c)))
    df = df.withColumn(c, F.when(limpio.isNull() | (limpio == ""), "SIN INFORMACION")
        .otherwise(limpio))

antes = df.count()
df = df.dropDuplicates(["ESTU_CONSECUTIVO"]).dropna(subset=["PUNT_GLOBAL"])
despues = df.count()
print(f"Registros antes: {antes}  |  despues: {despues}  |  eliminados: {antes - despues}")

# Nueva columna: nivel de desempeno segun terciles del puntaje global
q1, q2 = df.approxQuantile("PUNT_GLOBAL", [0.33, 0.66], 0.01)
df = df.withColumn("NIVEL_DESEMPENO",
    F.when(F.col("PUNT_GLOBAL") < q1, "BAJO")
    .when(F.col("PUNT_GLOBAL") < q2, "MEDIO")
    .otherwise("ALTO"))
print(f"Cortes de desempeno (terciles del puntaje global): BAJO < {q1} <= MEDIO < {q2} <= ALTO")

# Nueva columna: modulo con el puntaje mas bajo de cada estudiante
menor = F.least(*[F.col(c) for c in MODULOS])
expresion = None
for col, nombre in MODULOS.items():
    expresion = F.when(F.col(col) == menor, nombre) if expresion is None \
        else expresion.when(F.col(col) == menor, nombre)
df = df.withColumn("MODULO_MAS_BAJO", expresion)

df.cache()
df.select("PUNT_GLOBAL", "NIVEL_DESEMPENO", "MODULO_MAS_BAJO").show(5)

# ----------------------------------------------------------------------
# 3. Analisis exploratorio de datos (EDA)
# ----------------------------------------------------------------------
titulo("3.1 ESTADISTICAS DESCRIPTIVAS DE LOS PUNTAJES")
df.select(NUMERICAS).summary("count", "mean", "stddev", "min", "25%", "50%", "75%", "max").show()

titulo("3.2 PROMEDIO POR MODULO")
fila = df.select([F.round(F.avg(c), 2).alias(c) for c in MODULOS]).collect()[0]
prom_modulos = sorted([(MODULOS[c], fila[c]) for c in MODULOS], key=lambda x: x[1])
for nombre, valor in prom_modulos:
    print(f"  {nombre:<28} {valor}")


def promedio_por(columna, minimo=30, ordenar_por_nombre=False):
    resultado = (df.groupBy(columna)
    .agg(F.count("*").alias("ESTUDIANTES"),
        F.round(F.avg("PUNT_GLOBAL"), 2).alias("PROMEDIO_GLOBAL"))
    .filter(F.col("ESTUDIANTES") >= minimo))
    return resultado.orderBy(columna) if ordenar_por_nombre \
        else resultado.orderBy(F.desc("PROMEDIO_GLOBAL"))


titulo("3.3 PUNTAJE GLOBAL PROMEDIO POR DEPARTAMENTO DE LA INSTITUCION")
por_depto = promedio_por("ESTU_INST_DEPARTAMENTO")
por_depto.show(40, truncate=False)

titulo("3.4 PUNTAJE GLOBAL POR ORIGEN DE LA INSTITUCION")
por_origen = promedio_por("INST_ORIGEN")
por_origen.show(truncate=False)

titulo("3.5 PUNTAJE GLOBAL POR METODOLOGIA DEL PROGRAMA")
por_metodo = promedio_por("ESTU_METODO_PRGM")
por_metodo.show(truncate=False)

titulo("3.6 PUNTAJE GLOBAL POR ESTRATO DE VIVIENDA")
por_estrato = promedio_por("FAMI_ESTRATOVIVIENDA", ordenar_por_nombre=True)
por_estrato.show(truncate=False)

titulo("3.7 PUNTAJE GLOBAL SEGUN ACCESO A INTERNET Y GENERO")
promedio_por("FAMI_TIENEINTERNET").show(truncate=False)
promedio_por("ESTU_GENERO").show(truncate=False)

titulo("3.8 PUNTAJE GLOBAL SEGUN HORAS DE TRABAJO SEMANAL")
promedio_por("ESTU_HORASSEMANATRABAJA", ordenar_por_nombre=True).show(truncate=False)

titulo("3.9 MODULO CON EL PUNTAJE MAS BAJO (POR ESTUDIANTE)")
mas_bajo = df.groupBy("MODULO_MAS_BAJO").count().orderBy(F.desc("count"))
mas_bajo.show(truncate=False)

titulo("3.10 DISTRIBUCION POR NIVEL DE DESEMPENO")
df.groupBy("NIVEL_DESEMPENO").count().orderBy("NIVEL_DESEMPENO").show()

titulo("3.11 CORRELACION DE CADA MODULO CON EL PUNTAJE GLOBAL")
for col, nombre in MODULOS.items():
    print(f"  {nombre:<28} r = {df.stat.corr(col, 'PUNT_GLOBAL'):.3f}")

titulo("3.12 OPERACION CON RDD: ESTUDIANTES POR NIVEL DEL PROGRAMA (map + reduceByKey)")
rdd_niveles = (df.select("ESTU_NIVEL_PRGM_ACADEMICO").rdd
    .map(lambda fila: (fila[0], 1))
    .reduceByKey(lambda a, b: a + b))
for nivel, cantidad in sorted(rdd_niveles.collect(), key=lambda x: -x[1]):
    print(f"  {nivel:<40} {cantidad}")

# ----------------------------------------------------------------------
# 4. Visualizacion de resultados
# ----------------------------------------------------------------------
titulo("4. VISUALIZACION DE RESULTADOS")
import pandas as pd

pdf_mod = pd.DataFrame(prom_modulos, columns=["MODULO", "PROMEDIO"])
grafica_barras(pdf_mod, "MODULO", "PROMEDIO", "01_promedio_por_modulo.png",
    "Saber TyT 2019-2: puntaje promedio por modulo")

pdf_depto = por_depto.toPandas()
grafica_barras(pdf_depto, "ESTU_INST_DEPARTAMENTO", "PROMEDIO_GLOBAL",
    "02_promedio_por_departamento.png",
    "Puntaje global promedio por departamento de la institucion",
    horizontal=True)

pdf_origen = por_origen.toPandas()
grafica_barras(pdf_origen, "INST_ORIGEN", "PROMEDIO_GLOBAL", "03_promedio_por_origen.png",
    "Puntaje global promedio por origen de la institucion", color="#548235")

pdf_estrato = por_estrato.toPandas()
grafica_barras(pdf_estrato, "FAMI_ESTRATOVIVIENDA", "PROMEDIO_GLOBAL",
    "04_promedio_por_estrato.png",
    "Puntaje global promedio por estrato de vivienda", color="#c55a11")

# Histograma calculado en Spark (no se traen todos los datos al driver)
limites, frecuencias = df.select("PUNT_GLOBAL").rdd.map(lambda r: r[0]).histogram(20)
plt.figure(figsize=(10, 6))
plt.bar(limites[:-1], frecuencias, width=[b - a for a, b in zip(limites[:-1], limites[1:])],
        align="edge", color="#2e75b6", edgecolor="white")
plt.title("Distribucion del puntaje global - Saber TyT 2019-2")
plt.xlabel("Puntaje global")
plt.ylabel("Numero de estudiantes")
plt.tight_layout()
plt.savefig(os.path.join(CARPETA_GRAFICAS, "05_histograma_puntaje_global.png"), dpi=120)
plt.close()

# Tablas de resultados en CSV
pdf_depto.to_csv(os.path.join(CARPETA_RESULTADOS, "promedio_por_departamento.csv"), index=False)
pdf_origen.to_csv(os.path.join(CARPETA_RESULTADOS, "promedio_por_origen.csv"), index=False)
pdf_estrato.to_csv(os.path.join(CARPETA_RESULTADOS, "promedio_por_estrato.csv"), index=False)
pdf_mod.to_csv(os.path.join(CARPETA_RESULTADOS, "promedio_por_modulo.csv"), index=False)
print(f"Graficas guardadas en:    {CARPETA_GRAFICAS}")
print(f"Resultados guardados en:  {CARPETA_RESULTADOS}")

# ----------------------------------------------------------------------
# 5. Resumen de hallazgos
# ----------------------------------------------------------------------
titulo("5. RESUMEN DE HALLAZGOS")
print(f"- Estudiantes analizados: {despues}")
print(f"- Modulo con menor promedio: {prom_modulos[0][0]} ({prom_modulos[0][1]})")
print(f"- Modulo con mayor promedio: {prom_modulos[-1][0]} ({prom_modulos[-1][1]})")
if len(pdf_depto) > 0:
    print(f"- Departamento con mayor promedio: {pdf_depto.iloc[0, 0]} ({pdf_depto.iloc[0, 2]})")
    print(f"- Departamento con menor promedio: {pdf_depto.iloc[-1, 0]} ({pdf_depto.iloc[-1, 2]})")
top_bajo = mas_bajo.first()
print(f"- Modulo que con mas frecuencia es el peor de cada estudiante: "
    f"{top_bajo['MODULO_MAS_BAJO']} ({top_bajo['count']} estudiantes)")

spark.stop()
