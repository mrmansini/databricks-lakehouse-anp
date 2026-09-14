# Databricks notebook source
# Constroi as dimensoes de consumo: calendario, municipio e produto.
# As chaves sao hashes do valor natural, e nao sequenciais: a mesma entrada produz
# a mesma chave em qualquer execucao, o que permite comparar tabelas entre
# ambientes diferentes sem depender da ordem de carga.
# city_key usa o par UF mais nome porque o nome sozinho nao e chave: ha municipio
# homonimo em UFs distintas.
# Executado pelo job declarado em resources/job_gold.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

OBSERVATION = f"{catalog}.silver.price_observation"
DIM_DATE = f"{catalog}.gold.dim_date"
DIM_CITY = f"{catalog}.gold.dim_city"
DIM_PRODUCT = f"{catalog}.gold.dim_product"

# Nomes curados por rotulo publicado pela ANP. O rotulo cru fica na dimensao para
# que a origem continue rastreavel.
PRODUTOS = {
    "GASOLINA": ("Gasolina comum", "gasolina", "litro", True, True),
    "GASOLINA ADITIVADA": ("Gasolina aditivada", "gasolina", "litro", True, True),
    "ETANOL": ("Etanol hidratado", "etanol", "litro", True, True),
    "DIESEL": ("Oleo diesel", "diesel", "litro", True, True),
    "DIESEL S10": ("Oleo diesel S-10", "diesel", "litro", True, True),
    "GNV": ("Gas natural veicular", "gas natural", "metro cubico", False, False),
}

# COMMAND ----------

# O calendario cobre a janela das coletas com folga nas duas pontas, para que o
# join nunca perca observacao por borda de intervalo.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_DATE} AS
    SELECT
        full_date,
        extract(YEAROFWEEK FROM full_date) * 100 + weekofyear(full_date) AS survey_week_key,
        date_trunc('WEEK', full_date)::DATE                              AS week_start_date,
        extract(YEAROFWEEK FROM full_date)                               AS iso_year,
        weekofyear(full_date)                                            AS iso_week,
        date_trunc('MONTH', full_date)::DATE                             AS month_start_date,
        year(full_date)                                                  AS calendar_year,
        quarter(full_date)                                               AS calendar_quarter,
        dayofweek(full_date)                                             AS day_of_week
    FROM (
        SELECT explode(sequence(DATE'2022-12-26', DATE'2027-01-03', INTERVAL 1 DAY)) AS full_date
    )
    """
)

display(spark.sql(f"SELECT count(*) AS dias, min(full_date) AS de, max(full_date) AS ate FROM {DIM_DATE}"))

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_CITY} AS
    SELECT
        sha2(concat_ws('|', state, city), 256) AS city_key,
        city AS city_name,
        state AS uf,
        region,
        count(DISTINCT reseller_cnpj) AS stations
    FROM {OBSERVATION}
    GROUP BY ALL
    """
)

display(
    spark.sql(
        f"""
        SELECT
            count(*) AS municipios,
            count(DISTINCT city_name) AS nomes_distintos,
            count(DISTINCT uf) AS ufs
        FROM {DIM_CITY}
        """
    )
)

# COMMAND ----------

# Nomes homonimos em UFs diferentes: a razao de city_name nao servir como chave.
display(
    spark.sql(
        f"""
        SELECT city_name, collect_list(uf) AS ufs
        FROM {DIM_CITY}
        GROUP BY ALL HAVING count(*) > 1
        """
    )
)

# COMMAND ----------

mapa = ",\n        ".join(
    f"('{cru}', '{nome}', '{familia}', '{unidade}', {str(liquido).lower()}, {str(escopo).lower()})"
    for cru, (nome, familia, unidade, liquido, escopo) in PRODUTOS.items()
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW produto_curado AS
    SELECT * FROM VALUES
        {mapa}
    AS t(source_product_name, product_name, fuel_family, unit_of_measure, is_liquid_fuel, in_default_scope)
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_PRODUCT} AS
    SELECT
        sha2(o.product, 256)                                  AS product_key,
        o.product                                             AS source_product_name,
        coalesce(c.product_name, o.product)                   AS product_name,
        c.fuel_family,
        c.unit_of_measure,
        coalesce(c.is_liquid_fuel, true)                      AS is_liquid_fuel,
        coalesce(c.in_default_scope, true)                    AS in_default_scope,
        c.product_name IS NOT NULL                            AS is_curated,
        o.observations
    FROM (
        SELECT product, count(*) AS observations
        FROM {OBSERVATION} GROUP BY ALL
    ) o
    LEFT JOIN produto_curado c ON c.source_product_name = o.product
    """
)

print("dim_product gravada")

# COMMAND ----------

# Rotulos sem nome curado: o mapa acima precisa ser completado com estes valores.
display(
    spark.sql(
        f"""
        SELECT source_product_name, observations
        FROM {DIM_PRODUCT}
        WHERE NOT is_curated
        ORDER BY observations DESC
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"SELECT source_product_name, product_name, fuel_family, in_default_scope, observations "
        f"FROM {DIM_PRODUCT} ORDER BY observations DESC"
    )
)
