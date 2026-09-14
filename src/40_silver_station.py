# Databricks notebook source
# Deduplica a silver pela chave natural e constroi a dimensao de postos com
# versionamento SCD2 a partir do historico completo de observacoes.
# A bandeira entra pelo rotulo canonico de silver.brand_alias: sem isso, a troca
# de rotulo no cadastro da ANP abriria versao nova sem mudanca no mundo real.
# A deduplicacao e determinista por construcao: as chaves com precos divergentes
# nao tem resposta no dado, entao a escolha e arbitraria, mas precisa ser
# reproduzivel. As linhas descartadas ficam em ops.dedup_discarded.
# O Delta nao tem o equivalente a uma constraint de exclusao por intervalo, entao
# a nao sobreposicao das vigencias e verificada apos a carga, nao garantida antes.
# Executado pelo job declarado em resources/job_silver.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

CLEAN = f"{catalog}.silver.price_clean"
OBSERVATION = f"{catalog}.silver.price_observation"
BRAND_ALIAS = f"{catalog}.silver.brand_alias"
DIM_STATION = f"{catalog}.silver.dim_station"
DISCARDED = f"{catalog}.ops.dedup_discarded"
SNAPSHOT_CONFLICTS = f"{catalog}.ops.station_snapshot_conflicts"

# Atributos que definem uma versao do posto. Mudanca em qualquer um abre versao nova.
ATRIBUTOS = [
    "reseller_name",
    "street_name",
    "street_number",
    "neighborhood",
    "postal_code",
    "city",
    "state",
    "region",
    "brand",
]

HASH_ATRIBUTOS = (
    "sha2(concat_ws('|', "
    + ", ".join(f"coalesce({a}, '')" for a in ATRIBUTOS)
    + "), 256)"
)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW ranqueado AS
    SELECT
        *,
        row_number() OVER (
            PARTITION BY collection_date, reseller_cnpj, product
            ORDER BY sale_price, reseller_name, source_file
        ) AS ordem_na_chave
    FROM {CLEAN}
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {OBSERVATION} AS
    SELECT * EXCEPT (ordem_na_chave) FROM ranqueado WHERE ordem_na_chave = 1
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DISCARDED} AS
    SELECT * EXCEPT (ordem_na_chave), current_timestamp() AS discarded_at
    FROM ranqueado WHERE ordem_na_chave > 1
    """
)

display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {CLEAN})       AS antes,
            (SELECT count(*) FROM {OBSERVATION}) AS depois,
            (SELECT count(*) FROM {DISCARDED})   AS descartadas
        """
    )
)

# COMMAND ----------

# O rotulo canonico substitui o rotulo cru antes de qualquer comparacao de versao.
spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW observacao_canonica AS
    SELECT
        o.* EXCEPT (brand),
        coalesce(a.canonical_brand, o.brand) AS brand
    FROM {OBSERVATION} o
    LEFT JOIN {BRAND_ALIAS} a ON a.source_brand = o.brand
    """
)

# COMMAND ----------

# Um posto aparece uma vez por produto em cada data. Se a grafia dos atributos
# variar entre produtos, o posto teria dois estados no mesmo dia. Medir antes de
# montar as versoes.
spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW snapshot_bruto AS
    SELECT DISTINCT
        reseller_cnpj,
        collection_date,
        {", ".join(ATRIBUTOS)},
        {HASH_ATRIBUTOS} AS attribute_hash
    FROM observacao_canonica
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {SNAPSHOT_CONFLICTS} AS
    SELECT reseller_cnpj, collection_date, count(*) AS estados_no_dia
    FROM snapshot_bruto
    GROUP BY ALL
    HAVING count(*) > 1
    """
)

display(
    spark.sql(
        f"""
        SELECT count(*) AS datas_com_estado_ambiguo,
               coalesce(sum(estados_no_dia) - count(*), 0) AS linhas_excedentes
        FROM {SNAPSHOT_CONFLICTS}
        """
    )
)

# COMMAND ----------

# Desempate pelo hash: arbitrario, porem estavel entre execucoes.
spark.sql(
    """
    CREATE OR REPLACE TEMPORARY VIEW snapshot_unico AS
    SELECT * EXCEPT (ordem_no_dia) FROM (
        SELECT
            *,
            row_number() OVER (
                PARTITION BY reseller_cnpj, collection_date
                ORDER BY attribute_hash
            ) AS ordem_no_dia
        FROM snapshot_bruto
    )
    WHERE ordem_no_dia = 1
    """
)

# COMMAND ----------

# Uma versao comeca quando o hash muda em relacao a coleta anterior do mesmo posto.
spark.sql(
    """
    CREATE OR REPLACE TEMPORARY VIEW mudanca AS
    SELECT * FROM (
        SELECT
            *,
            lag(attribute_hash) OVER (
                PARTITION BY reseller_cnpj ORDER BY collection_date
            ) AS hash_anterior
        FROM snapshot_unico
    )
    WHERE hash_anterior IS NULL OR hash_anterior <> attribute_hash
    """
)

# COMMAND ----------

# Vigencia semiaberta [valid_from, valid_to): o fim de uma versao e o inicio da
# seguinte, e a corrente termina em 9999-12-31.
# station_key derivada do par chave natural mais inicio de vigencia: a mesma
# entrada produz a mesma chave em qualquer execucao.
spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_STATION} AS
    SELECT
        sha2(concat_ws('|', reseller_cnpj, cast(collection_date AS STRING)), 256) AS station_key,
        reseller_cnpj,
        {", ".join(ATRIBUTOS)},
        attribute_hash,
        collection_date AS valid_from,
        coalesce(
            lead(collection_date) OVER (PARTITION BY reseller_cnpj ORDER BY collection_date),
            DATE'9999-12-31'
        ) AS valid_to,
        lead(collection_date) OVER (PARTITION BY reseller_cnpj ORDER BY collection_date) IS NULL
            AS is_current
    FROM mudanca
    """
)

print("dim_station gravada")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            count(*)                         AS versoes,
            count(DISTINCT reseller_cnpj)    AS postos,
            count(DISTINCT brand)            AS bandeiras,
            count_if(is_current)             AS versoes_correntes,
            min(valid_from)                  AS primeira_vigencia,
            max(valid_from)                  AS ultima_abertura
        FROM {DIM_STATION}
        """
    )
)

# COMMAND ----------

# Invariante 1: exatamente uma versao corrente por posto.
display(
    spark.sql(
        f"""
        SELECT count(*) AS postos_fora_da_regra FROM (
            SELECT reseller_cnpj FROM {DIM_STATION}
            GROUP BY ALL HAVING count_if(is_current) <> 1
        )
        """
    )
)

# COMMAND ----------

# Invariante 2: vigencias contiguas e sem sobreposicao dentro de cada posto.
display(
    spark.sql(
        f"""
        SELECT count(*) AS intervalos_inconsistentes FROM (
            SELECT
                reseller_cnpj,
                valid_to,
                lead(valid_from) OVER (PARTITION BY reseller_cnpj ORDER BY valid_from) AS proximo_inicio
            FROM {DIM_STATION}
        )
        WHERE proximo_inicio IS NOT NULL AND proximo_inicio <> valid_to
        """
    )
)

# COMMAND ----------

# Invariante 3: toda observacao encontra exatamente uma versao vigente.
# A contagem e de versoes distintas: a tabela de observacoes tem uma linha por
# produto, entao contar linhas do join mediria combustiveis, nao versoes.
display(
    spark.sql(
        f"""
        SELECT count(*) AS observacoes_sem_versao_unica FROM (
            SELECT o.reseller_cnpj, o.collection_date, count(DISTINCT d.station_key) AS versoes
            FROM {OBSERVATION} o
            LEFT JOIN {DIM_STATION} d
                ON d.reseller_cnpj = o.reseller_cnpj
               AND o.collection_date >= d.valid_from
               AND o.collection_date <  d.valid_to
            GROUP BY ALL
            HAVING count(DISTINCT d.station_key) <> 1
        )
        """
    )
)

# COMMAND ----------

# Mudancas de bandeira que sobraram apos a consolidacao: sao os eventos que a
# camada gold estuda.
display(
    spark.sql(
        f"""
        SELECT count(*) AS trocas_de_bandeira FROM (
            SELECT
                brand,
                lag(brand) OVER (PARTITION BY reseller_cnpj ORDER BY valid_from) AS bandeira_anterior
            FROM {DIM_STATION}
        )
        WHERE bandeira_anterior IS NOT NULL AND brand IS DISTINCT FROM bandeira_anterior
        """
    )
)
