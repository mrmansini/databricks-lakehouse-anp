# Databricks notebook source
# Constroi o fato de observacoes de preco e o rollup semanal por municipio.
# O fato nao guarda bandeira nem municipio: eles chegam pela versao vigente do
# posto na data da coleta, que e o que torna o estudo de evento possivel.
# Os percentis sao exatos, e nao aproximados: comparar mediana aproximada com a
# mediana exata de outra implementacao inventaria diferenca onde nao ha.
# Sem particionamento ou clustering nesta etapa: a tabela nasce simples e a
# otimizacao e medida depois, contra este estado como linha de base.
# Executado pelo job declarado em resources/job_gold.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

OBSERVATION = f"{catalog}.silver.price_observation"
DIM_STATION = f"{catalog}.silver.dim_station"
DIM_DATE = f"{catalog}.gold.dim_date"
DIM_CITY = f"{catalog}.gold.dim_city"
DIM_PRODUCT = f"{catalog}.gold.dim_product"
FACT = f"{catalog}.gold.fct_price_observation"
WEEKLY = f"{catalog}.gold.weekly_price"

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {FACT} AS
    SELECT
        o.collection_date,
        d.station_key,
        p.product_key,
        o.sale_price,
        o.source_file
    FROM {OBSERVATION} o
    JOIN {DIM_STATION} d
        ON d.reseller_cnpj = o.reseller_cnpj
       AND o.collection_date >= d.valid_from
       AND o.collection_date <  d.valid_to
    JOIN {DIM_PRODUCT} p
        ON p.source_product_name = o.product
    """
)

display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {OBSERVATION}) AS observacoes_silver,
            (SELECT count(*) FROM {FACT})        AS linhas_fato
        """
    )
)

# COMMAND ----------

# Chave composta do fato: o join com a dimensao versionada nao pode multiplicar linha.
display(
    spark.sql(
        f"""
        SELECT count(*) AS chaves_duplicadas FROM (
            SELECT collection_date, station_key, product_key
            FROM {FACT} GROUP BY ALL HAVING count(*) > 1
        )
        """
    )
)

# COMMAND ----------

# O rollup agrega por municipio, que vem do posto vigente, e por semana ISO.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {WEEKLY} AS
    SELECT
        dt.survey_week_key,
        dt.week_start_date,
        dt.iso_year,
        dt.month_start_date,
        c.city_key,
        f.product_key,
        count(*)                                             AS observations,
        count(DISTINCT s.reseller_cnpj)                      AS stations,
        percentile(f.sale_price, 0.10)                       AS p10_price,
        percentile(f.sale_price, 0.50)                       AS median_price,
        percentile(f.sale_price, 0.90)                       AS p90_price,
        min(f.sale_price)                                    AS min_price,
        max(f.sale_price)                                    AS max_price,
        avg(f.sale_price)                                    AS avg_price
    FROM {FACT} f
    JOIN {DIM_STATION} s ON s.station_key = f.station_key
    JOIN {DIM_CITY} c    ON c.city_key = sha2(concat_ws('|', s.state, s.city), 256)
    JOIN {DIM_DATE} dt   ON dt.full_date = f.collection_date
    GROUP BY ALL
    """
)

display(spark.sql(f"SELECT count(*) AS linhas_rollup FROM {WEEKLY}"))

# COMMAND ----------

# O rollup nao pode perder nem inventar observacao em relacao ao fato.
display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {FACT})                AS fato,
            (SELECT sum(observations) FROM {WEEKLY})     AS soma_no_rollup
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            min(week_start_date) AS primeira_semana,
            max(week_start_date) AS ultima_semana,
            count(DISTINCT survey_week_key) AS semanas,
            count(DISTINCT city_key) AS municipios,
            count(DISTINCT product_key) AS produtos
        FROM {WEEKLY}
        """
    )
)

# COMMAND ----------

# Paridade etanol sobre gasolina comum por UF, na ultima semana disponivel:
# e o indicador de cabecalho do projeto, util como conferencia de ponta a ponta.
spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW paridade AS
    SELECT
        c.uf,
        w.survey_week_key,
        max(CASE WHEN p.product_name = 'Etanol hidratado'  THEN w.median_price END) AS etanol,
        max(CASE WHEN p.product_name = 'Gasolina comum'    THEN w.median_price END) AS gasolina
    FROM {WEEKLY} w
    JOIN {DIM_CITY} c    ON c.city_key = w.city_key
    JOIN {DIM_PRODUCT} p ON p.product_key = w.product_key
    WHERE w.survey_week_key = (SELECT max(survey_week_key) FROM {WEEKLY})
    GROUP BY ALL
    """
)

display(
    spark.sql(
        """
        SELECT
            uf,
            round(100 * percentile(etanol / gasolina, 0.5), 1) AS paridade_mediana_pct,
            count(*) AS municipios
        FROM paridade
        WHERE etanol IS NOT NULL AND gasolina IS NOT NULL
        GROUP BY ALL
        ORDER BY paridade_mediana_pct DESC
        """
    )
)
