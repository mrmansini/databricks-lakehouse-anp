# Databricks notebook source
# Constroi o catalogo de bandeiras e o mapa de rotulos equivalentes.
# O mapa e curado a mao porque o dado nao distingue renomeacao de troca real: a
# troca de rotulo da ANP acontece posto a posto ao longo de anos, e nao numa data
# unica, entao nenhum criterio de simultaneidade a separaria de um movimento de
# mercado. Cada par abaixo precisa de justificativa registrada.
# BRANCA identifica posto sem bandeira e nunca e consolidada: as transicoes de e
# para esse rotulo sao os eventos que a camada gold estuda.
# Le a silver limpa e nao a tabela deduplicada: o mapa de rotulos precisa existir
# antes da dimensao de postos, que o consome.
# Executado pelo job declarado em resources/job_silver.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

CLEAN = f"{catalog}.silver.price_clean"
BRAND_ALIAS = f"{catalog}.silver.brand_alias"
DIM_BRAND = f"{catalog}.silver.dim_brand"

UNBRANDED = "BRANCA"

# Rotulos que designam a mesma distribuidora. Fora desta lista, toda mudanca de
# rotulo e tratada como evento real.
ALIASES = [
    ("VIBRA ENERGIA", "VIBRA", "mesma distribuidora, rotulo encurtado no cadastro"),
    ("ALESAT", "ALE", "mesma distribuidora, rotulo encurtado no cadastro"),
]

# COMMAND ----------

valores = ",\n        ".join(
    f"('{origem}', '{canonico}', '{nota}')" for origem, canonico, nota in ALIASES
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {BRAND_ALIAS} AS
    SELECT * FROM VALUES
        {valores}
    AS t(source_brand, canonical_brand, rationale)
    """
)

display(spark.sql(f"SELECT * FROM {BRAND_ALIAS}"))

# COMMAND ----------

# Um alias que aponte para outro alias produziria resultado dependente da ordem
# de aplicacao. A verificacao falha alto em vez de resolver em cadeia.
encadeados = spark.sql(
    f"""
    SELECT a.source_brand, a.canonical_brand
    FROM {BRAND_ALIAS} a
    JOIN {BRAND_ALIAS} b ON a.canonical_brand = b.source_brand
    """
).collect()

if encadeados:
    raise ValueError(f"alias encadeado no mapa: {encadeados}")

print("mapa de alias validado: sem encadeamento")

# COMMAND ----------

# Um rotulo canonico que nao exista no dado indica erro de digitacao no mapa.
ausentes = spark.sql(
    f"""
    SELECT DISTINCT a.source_brand, a.canonical_brand
    FROM {BRAND_ALIAS} a
    LEFT JOIN (SELECT DISTINCT brand FROM {CLEAN}) o
        ON o.brand = a.canonical_brand
    WHERE o.brand IS NULL
    """
).collect()

if ausentes:
    raise ValueError(f"rotulo canonico inexistente no dado: {ausentes}")

print("mapa de alias validado: todo canonico existe no dado")

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_BRAND} AS
    SELECT
        sha2(brand_name, 256) AS brand_key,
        brand_name,
        brand_name = '{UNBRANDED}' AS is_unbranded,
        stations
    FROM (
        SELECT
            coalesce(a.canonical_brand, o.brand) AS brand_name,
            count(DISTINCT o.reseller_cnpj)      AS stations
        FROM {CLEAN} o
        LEFT JOIN {BRAND_ALIAS} a ON a.source_brand = o.brand
        GROUP BY ALL
    )
    """
)

print("dim_brand gravada")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(DISTINCT brand) FROM {CLEAN}) AS rotulos_crus,
            (SELECT count(*) FROM {BRAND_ALIAS})              AS alias_aplicados,
            (SELECT count(*) FROM {DIM_BRAND})                AS bandeiras_canonicas
        """
    )
)

# COMMAND ----------

display(
    spark.sql(f"SELECT brand_name, stations, is_unbranded FROM {DIM_BRAND} ORDER BY stations DESC LIMIT 15")
)
