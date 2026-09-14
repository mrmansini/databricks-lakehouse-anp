# Databricks notebook source
# Compara o mesmo rollup semanal em Spark e em pandas, no mesmo compute.
# Duas implementacoes em pandas sao medidas: uma com funcao Python dentro do
# agregador e outra com quantil vetorizado. A diferenca entre elas fica registrada
# em vez de corrigida em silencio, porque e ela que mostra que uma comparacao
# entre motores mede a implementacao antes de medir o motor.
# O custo e separado em preparo e agregacao: somar os dois esconderia o trade-off
# que a comparacao existe para expor.
# O criterio de validade e a igualdade do resultado: sem o mesmo numero de grupos
# e as mesmas medianas, o tempo nao significa nada.
# Executado pelo job declarado em resources/job_ops.yml.

# COMMAND ----------

import statistics
import time
import uuid
from datetime import datetime, timezone

import pandas as pd

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text("repeticoes", "3")

catalog = dbutils.widgets.get("catalog")
repeticoes = int(dbutils.widgets.get("repeticoes"))

RUN_ID = uuid.uuid4().hex[:12]

FACT = f"{catalog}.gold.fct_price_observation"
DIM_STATION = f"{catalog}.silver.dim_station"
RUNS = f"{catalog}.ops.benchmark_runs"

CHAVES = ["week_start_date", "state", "city", "product_key"]

# COMMAND ----------

detalhe = spark.sql(f"DESCRIBE DETAIL {FACT}").collect()[0]
print("pandas", pd.__version__)
print(f"fato: {detalhe['numFiles']} arquivo(s), {detalhe['sizeInBytes'] / 1e6:.1f} MB em Parquet")

medidas = []


def registra(fase, consulta, tempos):
    """Guarda a mediana e os extremos de uma serie de execucoes."""
    medidas.append(
        (
            RUN_ID, fase, consulta, len(tempos),
            statistics.median(tempos), min(tempos), max(tempos),
            detalhe["numFiles"], detalhe["sizeInBytes"], "",
            datetime.now(timezone.utc),
        )
    )
    print(f"{fase} · {consulta}: mediana {statistics.median(tempos):.2f}s de {[round(t, 2) for t in tempos]}")


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

# Aquecimento: a primeira execucao compila o plano e mede o ambiente.
spark.sql(SQL_ROLLUP).count()

tempos = []
for _ in range(repeticoes):
    t0 = time.perf_counter()
    grupos_spark = spark.sql(SQL_ROLLUP).count()
    tempos.append(time.perf_counter() - t0)

registra("spark", "join_e_agregacao", tempos)
print(f"grupos: {grupos_spark:,}")

# COMMAND ----------

t0 = time.perf_counter()
fato_pd = spark.table(FACT).select(
    "collection_date", "station_key", "product_key", "sale_price"
).toPandas()
station_pd = spark.table(DIM_STATION).select(
    "station_key", "reseller_cnpj", "state", "city"
).toPandas()
registra("pandas", "carga_para_memoria", [time.perf_counter() - t0])

print(f"fato em pandas: {len(fato_pd):,} linhas, {fato_pd.memory_usage(deep=True).sum() / 1e6:.1f} MB")

# COMMAND ----------


def prepara():
    """Junta as tabelas e deriva a semana. Equivale ao join do lado Spark."""
    df = fato_pd.merge(station_pd, on="station_key", how="inner")
    dias = pd.to_datetime(df["collection_date"])
    df["week_start_date"] = (dias - pd.to_timedelta(dias.dt.dayofweek, unit="D")).dt.date
    df["sale_price"] = df["sale_price"].astype("float64")
    return df


tempos = []
for _ in range(repeticoes):
    t0 = time.perf_counter()
    preparado = prepara()
    tempos.append(time.perf_counter() - t0)

registra("pandas", "preparo", tempos)

# COMMAND ----------


def agrega_com_lambda(df):
    """Quantis por funcao Python: o interpretador e chamado uma vez por grupo."""
    return df.groupby(CHAVES, sort=False).agg(
        observations=("sale_price", "size"),
        stations=("reseller_cnpj", "nunique"),
        p10_price=("sale_price", lambda x: x.quantile(0.10)),
        median_price=("sale_price", "median"),
        p90_price=("sale_price", lambda x: x.quantile(0.90)),
        min_price=("sale_price", "min"),
        max_price=("sale_price", "max"),
        avg_price=("sale_price", "mean"),
    )


def agrega_vetorizado(df):
    """Mesmo resultado com quantil vetorizado, sem funcao Python por grupo."""
    precos = df.groupby(CHAVES, sort=False)["sale_price"]
    base = precos.agg(
        observations="size",
        median_price="median",
        min_price="min",
        max_price="max",
        avg_price="mean",
    )
    quantis = precos.quantile([0.10, 0.90]).unstack()
    quantis.columns = ["p10_price", "p90_price"]
    postos = df.groupby(CHAVES, sort=False)["reseller_cnpj"].nunique().rename("stations")
    return base.join(quantis).join(postos)


# COMMAND ----------

# Uma execucao so: a versao com lambda custa ordens de grandeza a mais.
t0 = time.perf_counter()
resultado_lambda = agrega_com_lambda(preparado)
registra("pandas_com_lambda", "agregacao", [time.perf_counter() - t0])

# COMMAND ----------

agrega_vetorizado(preparado)

tempos = []
for _ in range(repeticoes):
    t0 = time.perf_counter()
    resultado_vetorizado = agrega_vetorizado(preparado)
    tempos.append(time.perf_counter() - t0)

registra("pandas_vetorizado", "agregacao", tempos)

# COMMAND ----------

colunas = [
    "run_id", "fase", "consulta", "execucoes", "mediana_seg", "minimo_seg",
    "maximo_seg", "num_files", "size_bytes", "clustering_cols", "medido_em",
]
spark.createDataFrame(medidas, colunas).write.mode("append").saveAsTable(RUNS)

display(
    spark.sql(
        f"""
        SELECT fase, consulta, execucoes, round(mediana_seg, 2) AS mediana_seg
        FROM {RUNS} WHERE run_id = '{RUN_ID}'
        ORDER BY mediana_seg
        """
    )
)

# COMMAND ----------

display(
    spark.createDataFrame(
        pd.DataFrame(
            {
                "implementacao": [
                    "spark (join + agregacao)",
                    "pandas vetorizado (carga + preparo + agregacao)",
                    "pandas com lambda (carga + preparo + agregacao)",
                ],
                "grupos": [
                    grupos_spark,
                    len(resultado_vetorizado),
                    len(resultado_lambda),
                ],
            }
        )
    )
)

# COMMAND ----------

# Conferencia de valor, e nao so de contagem: as medianas precisam coincidir.
amostra_spark = (
    spark.sql(SQL_ROLLUP)
    .selectExpr(
        "week_start_date", "state", "city", "product_key",
        "cast(median_price AS DOUBLE) AS median_spark",
        "cast(p10_price AS DOUBLE) AS p10_spark",
    )
    .toPandas()
)

comparado = amostra_spark.merge(
    resultado_vetorizado.reset_index()[CHAVES + ["median_price", "p10_price"]].rename(
        columns={"median_price": "median_pandas", "p10_price": "p10_pandas"}
    ),
    on=CHAVES,
    how="outer",
    indicator=True,
)

comparado["dif_mediana"] = (comparado["median_spark"] - comparado["median_pandas"]).abs()
comparado["dif_p10"] = (comparado["p10_spark"] - comparado["p10_pandas"]).abs()

divergentes = comparado[
    (comparado["_merge"] != "both")
    | (comparado["dif_mediana"] > 1e-6)
    | (comparado["dif_p10"] > 1e-6)
]

display(
    spark.createDataFrame(
        pd.DataFrame(
            {
                "metrica": ["grupos comparados", "grupos divergentes", "maior diferenca na mediana", "maior diferenca no p10"],
                "valor": [
                    float(len(comparado)),
                    float(len(divergentes)),
                    float(comparado["dif_mediana"].max()),
                    float(comparado["dif_p10"].max()),
                ],
            }
        )
    )
)

# COMMAND ----------

if len(divergentes):
    display(spark.createDataFrame(divergentes.head(20).astype(str)))
