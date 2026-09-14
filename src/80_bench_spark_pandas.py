# Databricks notebook source
# Compara o mesmo rollup semanal em Spark e em pandas, no mesmo compute.
# O custo e medido em duas partes separadas: trazer o dado para a memoria e
# calcular. Somar as duas escondeira justamente o que a comparacao quer mostrar.
# O criterio de validade e a igualdade do resultado: se as duas implementacoes nao
# produzirem o mesmo numero de grupos e as mesmas medianas, o tempo nao significa
# nada.
# Executado pelo job declarado em resources/job_ops.yml.

# COMMAND ----------

import statistics
import time
import uuid
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text("repeticoes", "3")

catalog = dbutils.widgets.get("catalog")
repeticoes = int(dbutils.widgets.get("repeticoes"))

RUN_ID = uuid.uuid4().hex[:12]

FACT = f"{catalog}.gold.fct_price_observation"
DIM_STATION = f"{catalog}.silver.dim_station"
RUNS = f"{catalog}.ops.benchmark_runs"

# COMMAND ----------

import pandas as pd

print("pandas", pd.__version__)

detalhe = spark.sql(f"DESCRIBE DETAIL {FACT}").collect()[0]
print(f"fato: {detalhe['numFiles']} arquivo(s), {detalhe['sizeInBytes'] / 1e6:.1f} MB em Parquet")
print(f"linhas: {spark.table(FACT).count():,}")

# COMMAND ----------

SQL_ROLLUP = f"""
    SELECT
        date_trunc('WEEK', f.collection_date)::DATE AS week_start_date,
        s.state,
        s.city,
        f.product_key,
        count(*)                        AS observations,
        count(DISTINCT s.reseller_cnpj) AS stations,
        percentile(f.sale_price, 0.10)  AS p10_price,
        percentile(f.sale_price, 0.50)  AS median_price,
        percentile(f.sale_price, 0.90)  AS p90_price,
        min(f.sale_price)               AS min_price,
        max(f.sale_price)               AS max_price,
        avg(f.sale_price)               AS avg_price
    FROM {FACT} f
    JOIN {DIM_STATION} s ON s.station_key = f.station_key
    GROUP BY ALL
"""


def rollup_spark():
    """Executa o rollup no Spark e materializa o resultado."""
    return spark.sql(SQL_ROLLUP).count()


# COMMAND ----------

# Aquecimento: a primeira execucao carrega metadados e compila o plano, e mede o
# ambiente em vez da consulta.
rollup_spark()

tempos_spark = []
for _ in range(repeticoes):
    t0 = time.perf_counter()
    grupos_spark = rollup_spark()
    tempos_spark.append(time.perf_counter() - t0)

print(f"spark: {grupos_spark:,} grupos · tempos {[round(t, 2) for t in tempos_spark]}")

# COMMAND ----------

t0 = time.perf_counter()
fato_pd = spark.table(FACT).select(
    "collection_date", "station_key", "product_key", "sale_price"
).toPandas()
station_pd = spark.table(DIM_STATION).select(
    "station_key", "reseller_cnpj", "state", "city"
).toPandas()
tempo_carga = time.perf_counter() - t0

print(f"carga para memoria: {tempo_carga:.2f}s")
print(f"fato em pandas: {len(fato_pd):,} linhas, {fato_pd.memory_usage(deep=True).sum() / 1e6:.1f} MB")

# COMMAND ----------


def rollup_pandas():
    """Mesmo rollup em pandas, sobre dados ja residentes na memoria."""
    df = fato_pd.merge(station_pd, on="station_key", how="inner")
    dias = pd.to_datetime(df["collection_date"])
    df["week_start_date"] = (dias - pd.to_timedelta(dias.dt.dayofweek, unit="D")).dt.date
    df["sale_price"] = df["sale_price"].astype("float64")

    agrupado = df.groupby(["week_start_date", "state", "city", "product_key"], sort=False)
    resultado = agrupado.agg(
        observations=("sale_price", "size"),
        stations=("reseller_cnpj", "nunique"),
        p10_price=("sale_price", lambda x: x.quantile(0.10)),
        median_price=("sale_price", "median"),
        p90_price=("sale_price", lambda x: x.quantile(0.90)),
        min_price=("sale_price", "min"),
        max_price=("sale_price", "max"),
        avg_price=("sale_price", "mean"),
    )
    return resultado


# COMMAND ----------

rollup_pandas()

tempos_pandas = []
for _ in range(repeticoes):
    t0 = time.perf_counter()
    resultado_pd = rollup_pandas()
    tempos_pandas.append(time.perf_counter() - t0)

grupos_pandas = len(resultado_pd)
print(f"pandas: {grupos_pandas:,} grupos · tempos {[round(t, 2) for t in tempos_pandas]}")

# COMMAND ----------

# Sem resultado igual, a comparacao de tempo nao significa nada.
print(f"grupos spark:  {grupos_spark:,}")
print(f"grupos pandas: {grupos_pandas:,}")
print(f"identicos: {grupos_spark == grupos_pandas}")

mediana_spark = statistics.median(tempos_spark)
mediana_pandas = statistics.median(tempos_pandas)

print()
print(f"spark (calculo):            {mediana_spark:.2f}s")
print(f"pandas (calculo):           {mediana_pandas:.2f}s")
print(f"pandas (carga + calculo):   {tempo_carga + mediana_pandas:.2f}s")

# COMMAND ----------

registros = [
    (RUN_ID, "spark_rollup", "calculo", len(tempos_spark), mediana_spark,
     min(tempos_spark), max(tempos_spark), detalhe["numFiles"], detalhe["sizeInBytes"],
     "", datetime.now(timezone.utc)),
    (RUN_ID, "pandas_rollup", "calculo", len(tempos_pandas), mediana_pandas,
     min(tempos_pandas), max(tempos_pandas), detalhe["numFiles"], detalhe["sizeInBytes"],
     "", datetime.now(timezone.utc)),
    (RUN_ID, "pandas_rollup", "carga_para_memoria", 1, tempo_carga,
     tempo_carga, tempo_carga, detalhe["numFiles"], detalhe["sizeInBytes"],
     "", datetime.now(timezone.utc)),
]

colunas = [
    "run_id", "fase", "consulta", "execucoes", "mediana_seg", "minimo_seg",
    "maximo_seg", "num_files", "size_bytes", "clustering_cols", "medido_em",
]
spark.createDataFrame(registros, colunas).write.mode("append").saveAsTable(RUNS)

display(spark.sql(f"SELECT * FROM {RUNS} WHERE run_id = '{RUN_ID}'"))

# COMMAND ----------

# Conferencia de valor, e nao so de contagem: as medianas precisam coincidir.
amostra_spark = (
    spark.sql(SQL_ROLLUP)
    .selectExpr("week_start_date", "state", "city", "product_key",
                "cast(median_price AS DOUBLE) AS median_spark", "observations")
    .toPandas()
)

comparado = amostra_spark.merge(
    resultado_pd.reset_index()[
        ["week_start_date", "state", "city", "product_key", "median_price"]
    ].rename(columns={"median_price": "median_pandas"}),
    on=["week_start_date", "state", "city", "product_key"],
    how="outer",
    indicator=True,
)

divergentes = comparado[
    (comparado["_merge"] != "both")
    | ((comparado["median_spark"] - comparado["median_pandas"]).abs() > 1e-6)
]

print(f"grupos comparados: {len(comparado):,}")
print(f"grupos divergentes: {len(divergentes):,}")
if len(divergentes):
    display(divergentes.head(20))
