# Databricks notebook source
# Le a camada bronze, descarta linhas em branco, converte tipos e normaliza texto.
# A conversao usa try_to_date e try_cast: o objetivo nao e tolerar erro, e captura-lo.
# O que nao converte vai para ops.silver_rejects com o valor original, em vez de
# derrubar o job ou desaparecer.
# A deduplicacao nao acontece aqui: ops.silver_key_conflicts mede se as chaves
# repetidas sao identicas ou divergem no preco, e a regra e decidida na camada gold.
# Executado pelo job declarado em resources/job_silver.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

BRONZE = f"{catalog}.bronze.price_raw"
SILVER = f"{catalog}.silver.price_clean"
REJECTS = f"{catalog}.ops.silver_rejects"
CONFLICTS = f"{catalog}.ops.silver_key_conflicts"

# Todas as colunas de dado da bronze. Uma linha com todas nulas e linha em branco
# no arquivo de origem, nao coleta sem preco.
COLUNAS_DADO = [
    "region",
    "state",
    "city",
    "reseller_name",
    "reseller_cnpj",
    "street_name",
    "street_number",
    "complement",
    "neighborhood",
    "postal_code",
    "product",
    "collection_date",
    "sale_price",
    "purchase_price",
    "unit_of_measure",
    "brand",
]

LINHA_EM_BRANCO = " AND ".join(f"{c} IS NULL" for c in COLUNAS_DADO)

# COMMAND ----------

# A conversao de texto para tipo fica isolada numa view: a mesma expressao alimenta
# a tabela limpa e a tabela de rejeitos, sem risco de as duas divergirem.
spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW convertido AS
    SELECT
        upper(trim(region))                                          AS region,
        upper(trim(state))                                           AS state,
        trim(city)                                                   AS city,
        trim(reseller_name)                                          AS reseller_name,
        regexp_replace(reseller_cnpj, '[^0-9]', '')                  AS reseller_cnpj,
        trim(street_name)                                            AS street_name,
        trim(street_number)                                          AS street_number,
        trim(neighborhood)                                           AS neighborhood,
        regexp_replace(postal_code, '[^0-9]', '')                    AS postal_code,
        trim(product)                                                AS product,
        try_to_date(collection_date, 'dd/MM/yyyy')                   AS collection_date,
        try_cast(replace(sale_price, ',', '.') AS DECIMAL(10,3))     AS sale_price,
        try_cast(replace(purchase_price, ',', '.') AS DECIMAL(10,3)) AS purchase_price,
        trim(unit_of_measure)                                        AS unit_of_measure,
        upper(trim(brand))                                           AS brand,
        collection_date                                              AS collection_date_raw,
        sale_price                                                   AS sale_price_raw,
        reseller_cnpj                                                AS reseller_cnpj_raw,
        source_file,
        ingested_at
    FROM {BRONZE}
    WHERE NOT ({LINHA_EM_BRANCO})
    """
)

# COMMAND ----------

# Uma linha e rejeitada quando um campo obrigatorio nao existe ou nao converteu.
CRITERIO_REJEITO = """
    collection_date IS NULL
    OR sale_price IS NULL
    OR reseller_cnpj IS NULL OR length(reseller_cnpj) <> 14
    OR product IS NULL
"""

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {REJECTS} AS
    SELECT
        source_file,
        collection_date_raw,
        sale_price_raw,
        reseller_cnpj_raw,
        product,
        CASE
            WHEN collection_date IS NULL THEN 'data nao convertida'
            WHEN sale_price IS NULL THEN 'preco nao convertido'
            WHEN reseller_cnpj IS NULL OR length(reseller_cnpj) <> 14 THEN 'cnpj fora do formato'
            ELSE 'produto ausente'
        END AS motivo,
        current_timestamp() AS evaluated_at
    FROM convertido
    WHERE {CRITERIO_REJEITO}
    """
)

display(
    spark.sql(f"SELECT motivo, count(*) AS linhas FROM {REJECTS} GROUP BY ALL ORDER BY linhas DESC")
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SILVER} AS
    SELECT
        region,
        state,
        city,
        reseller_name,
        reseller_cnpj,
        street_name,
        street_number,
        neighborhood,
        postal_code,
        product,
        collection_date,
        sale_price,
        purchase_price,
        unit_of_measure,
        brand,
        source_file,
        ingested_at
    FROM convertido
    WHERE NOT ({CRITERIO_REJEITO})
    """
)

print("silver gravada")

# COMMAND ----------

# Chave natural do projeto: data da coleta, posto e produto. Medir antes de
# deduplicar: repeticao identica e repeticao com precos divergentes pedem
# tratamentos diferentes.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {CONFLICTS} AS
    SELECT
        collection_date,
        reseller_cnpj,
        product,
        count(*)                    AS ocorrencias,
        count(DISTINCT sale_price)  AS precos_distintos,
        min(sale_price)             AS menor_preco,
        max(sale_price)             AS maior_preco
    FROM {SILVER}
    GROUP BY ALL
    HAVING count(*) > 1
    """
)

display(
    spark.sql(
        f"""
        SELECT
            count(*)                                  AS chaves_repetidas,
            sum(ocorrencias) - count(*)               AS linhas_excedentes,
            count_if(precos_distintos > 1)            AS chaves_com_preco_divergente
        FROM {CONFLICTS}
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {BRONZE})   AS bronze,
            (SELECT count(*) FROM {REJECTS})  AS rejeitadas,
            (SELECT count(*) FROM {SILVER})   AS silver,
            (SELECT count(*) FROM {SILVER})
                - (SELECT coalesce(sum(ocorrencias) - count(*), 0) FROM {CONFLICTS})
                                              AS silver_apos_dedup_prevista
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            min(collection_date) AS primeira_coleta,
            max(collection_date) AS ultima_coleta,
            count(DISTINCT reseller_cnpj) AS postos,
            count(DISTINCT product) AS produtos,
            count(DISTINCT brand) AS bandeiras,
            count(DISTINCT concat(state, '|', city)) AS municipios
        FROM {SILVER}
        """
    )
)
