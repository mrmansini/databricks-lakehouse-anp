# Databricks notebook source
# Monta a dimensao de postos em duas etapas e compara com a versao montada de uma
# vez so: historico ate uma data de corte, depois o restante aplicado por MERGE,
# uma data de coleta por vez.
# O MERGE do Delta recusa quando a mesma chave aparece mais de uma vez na origem.
# Um posto que muda duas vezes dentro do periodo incremental quebraria a operacao,
# entao a aplicacao e por data, que tambem e como o fluxo rodaria em producao.
# O comando usa duas passadas na mesma origem: uma linha com a chave do posto
# fecha a versao vigente, e uma segunda linha com chave nula, que nunca casa,
# insere a versao nova.
# A tabela do experimento e separada da original, entao a comparacao ja validada
# nao corre risco.
# Executado pelo job declarado em resources/job_ops.yml.

# COMMAND ----------

import time
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text("data_corte", "2026-01-01")

catalog = dbutils.widgets.get("catalog")
data_corte = dbutils.widgets.get("data_corte")

OBSERVATION = f"{catalog}.silver.price_observation"
BRAND_ALIAS = f"{catalog}.silver.brand_alias"
DIM_LOTE = f"{catalog}.silver.dim_station"
DIM_MERGE = f"{catalog}.ops.dim_station_merge"
RUNS = f"{catalog}.ops.benchmark_runs"

ATRIBUTOS = [
    "reseller_name", "street_name", "street_number", "neighborhood",
    "postal_code", "city", "state", "region", "brand",
]

HASH_ATRIBUTOS = (
    "sha2(concat_ws('|', " + ", ".join(f"coalesce({a}, '')" for a in ATRIBUTOS) + "), 256)"
)

COLS_ATRIB = ", ".join(ATRIBUTOS)

# COMMAND ----------

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW observacao_canonica AS
    SELECT o.* EXCEPT (brand), coalesce(a.canonical_brand, o.brand) AS brand
    FROM {OBSERVATION} o
    LEFT JOIN {BRAND_ALIAS} a ON a.source_brand = o.brand
    """
)

spark.sql(
    f"""
    CREATE OR REPLACE TEMPORARY VIEW snapshot_unico AS
    SELECT * EXCEPT (ordem_no_dia) FROM (
        SELECT *, row_number() OVER (
            PARTITION BY reseller_cnpj, collection_date ORDER BY attribute_hash
        ) AS ordem_no_dia
        FROM (
            SELECT DISTINCT
                reseller_cnpj, collection_date, {COLS_ATRIB},
                {HASH_ATRIBUTOS} AS attribute_hash
            FROM observacao_canonica
        )
    ) WHERE ordem_no_dia = 1
    """
)

print("snapshots preparados")

# COMMAND ----------

# Etapa 1: historico ate a data de corte, montado de uma vez.
t0 = time.perf_counter()

spark.sql(
    f"""
    CREATE OR REPLACE TABLE {DIM_MERGE} AS
    WITH mudanca AS (
        SELECT * FROM (
            SELECT *, lag(attribute_hash) OVER (
                PARTITION BY reseller_cnpj ORDER BY collection_date
            ) AS hash_anterior
            FROM snapshot_unico
            WHERE collection_date < DATE'{data_corte}'
        )
        WHERE hash_anterior IS NULL OR hash_anterior <> attribute_hash
    )
    SELECT
        sha2(concat_ws('|', reseller_cnpj, cast(collection_date AS STRING)), 256) AS station_key,
        reseller_cnpj,
        {COLS_ATRIB},
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

tempo_carga_inicial = time.perf_counter() - t0
versoes_iniciais = spark.table(DIM_MERGE).count()
print(f"carga inicial ate {data_corte}: {versoes_iniciais:,} versoes em {tempo_carga_inicial:.1f}s")

# COMMAND ----------

datas = [
    linha[0].isoformat()
    for linha in spark.sql(
        f"""
        SELECT DISTINCT collection_date FROM snapshot_unico
        WHERE collection_date >= DATE'{data_corte}' ORDER BY collection_date
        """
    ).collect()
]

print(f"datas a aplicar por MERGE: {len(datas)}")
print(f"primeira: {datas[0]} · ultima: {datas[-1]}")

# COMMAND ----------

VALORES_INSERT = ", ".join(f"s.{a}" for a in ATRIBUTOS)


def aplica_lote(data):
    """Aplica um dia de coleta na dimensao pelo padrao SCD2 com MERGE."""
    sql = f"""
    MERGE INTO {DIM_MERGE} d
    USING (
        WITH lote AS (
            SELECT * FROM snapshot_unico WHERE collection_date = DATE'{data}'
        ),
        -- Apenas postos novos ou com atributos diferentes da versao vigente.
        -- Linhas sem mudanca nao produziriam efeito e so aumentariam o trabalho.
        mudou AS (
            SELECT l.*
            FROM lote l
            LEFT JOIN {DIM_MERGE} c
                ON c.reseller_cnpj = l.reseller_cnpj AND c.is_current
            WHERE c.reseller_cnpj IS NULL OR c.attribute_hash <> l.attribute_hash
        )
        SELECT reseller_cnpj AS merge_key, reseller_cnpj, collection_date,
               {COLS_ATRIB}, attribute_hash
        FROM mudou
        UNION ALL
        SELECT NULL AS merge_key, m.reseller_cnpj, m.collection_date,
               {", ".join(f"m.{a}" for a in ATRIBUTOS)}, m.attribute_hash
        FROM mudou m
        JOIN {DIM_MERGE} c
            ON c.reseller_cnpj = m.reseller_cnpj AND c.is_current
    ) s
    ON d.reseller_cnpj = s.merge_key AND d.is_current
    WHEN MATCHED AND d.attribute_hash <> s.attribute_hash THEN
        UPDATE SET d.is_current = false, d.valid_to = s.collection_date
    WHEN NOT MATCHED THEN
        INSERT (station_key, reseller_cnpj, {COLS_ATRIB}, attribute_hash,
                valid_from, valid_to, is_current)
        VALUES (
            sha2(concat_ws('|', s.reseller_cnpj, cast(s.collection_date AS STRING)), 256),
            s.reseller_cnpj, {VALORES_INSERT}, s.attribute_hash,
            s.collection_date, DATE'9999-12-31', true
        )
    """
    return spark.sql(sql)


# COMMAND ----------

t0 = time.perf_counter()
tempos_por_lote = []

for i, data in enumerate(datas, start=1):
    t_lote = time.perf_counter()
    aplica_lote(data)
    tempos_por_lote.append(time.perf_counter() - t_lote)
    if i % 20 == 0 or i == len(datas):
        print(f"  {i}/{len(datas)} lotes · acumulado {time.perf_counter() - t0:.0f}s")

tempo_incremental = time.perf_counter() - t0
print(f"\n{len(datas)} lotes aplicados em {tempo_incremental:.1f}s")
print(f"tempo medio por lote: {tempo_incremental / len(datas):.2f}s")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM {DIM_LOTE})  AS versoes_uma_vez,
            (SELECT count(*) FROM {DIM_MERGE}) AS versoes_incremental
        """
    )
)

# COMMAND ----------

# A comparacao usa a chave natural com o inicio da vigencia, mais os campos que
# definem a versao. Contagem igual com conteudo diferente nao serve.
display(
    spark.sql(
        f"""
        SELECT 'apenas na versao de uma vez' AS lado, count(*) AS linhas FROM (
            SELECT reseller_cnpj, valid_from, valid_to, attribute_hash, is_current
            FROM {DIM_LOTE}
            EXCEPT
            SELECT reseller_cnpj, valid_from, valid_to, attribute_hash, is_current
            FROM {DIM_MERGE}
        )
        UNION ALL
        SELECT 'apenas na versao incremental', count(*) FROM (
            SELECT reseller_cnpj, valid_from, valid_to, attribute_hash, is_current
            FROM {DIM_MERGE}
            EXCEPT
            SELECT reseller_cnpj, valid_from, valid_to, attribute_hash, is_current
            FROM {DIM_LOTE}
        )
        """
    )
)

# COMMAND ----------

# As mesmas regras que valem para a dimensao original.
display(
    spark.sql(
        f"""
        SELECT
            (SELECT count(*) FROM (
                SELECT reseller_cnpj FROM {DIM_MERGE}
                GROUP BY ALL HAVING count_if(is_current) <> 1
            )) AS postos_sem_versao_vigente_unica,
            (SELECT count(*) FROM (
                SELECT reseller_cnpj, valid_to,
                       lead(valid_from) OVER (
                           PARTITION BY reseller_cnpj ORDER BY valid_from
                       ) AS proximo_inicio
                FROM {DIM_MERGE}
            ) WHERE proximo_inicio IS NOT NULL AND proximo_inicio <> valid_to)
                AS intervalos_inconsistentes
        """
    )
)

# COMMAND ----------

registros = [
    ("merge_incremental", "carga_inicial_ate_corte", 1, tempo_carga_inicial,
     tempo_carga_inicial, tempo_carga_inicial, 0, 0, data_corte,
     datetime.now(timezone.utc)),
    ("merge_incremental", f"{len(datas)}_lotes_por_data", len(datas),
     tempo_incremental / len(datas), min(tempos_por_lote), max(tempos_por_lote),
     0, 0, data_corte, datetime.now(timezone.utc)),
    ("merge_incremental", "total", 1, tempo_carga_inicial + tempo_incremental,
     tempo_carga_inicial + tempo_incremental, tempo_carga_inicial + tempo_incremental,
     0, 0, data_corte, datetime.now(timezone.utc)),
]

colunas = [
    "fase", "consulta", "execucoes", "mediana_seg", "minimo_seg", "maximo_seg",
    "num_files", "size_bytes", "clustering_cols", "medido_em",
]

import uuid
run_id = uuid.uuid4().hex[:12]
df = spark.createDataFrame(registros, colunas).selectExpr(
    f"'{run_id}' AS run_id", "*"
)
df.write.mode("append").saveAsTable(RUNS)

display(df)

# COMMAND ----------

# Time travel: o historico da tabela guarda cada lote aplicado.
display(spark.sql(f"SELECT count(*) AS versoes_delta FROM (DESCRIBE HISTORY {DIM_MERGE})"))

# COMMAND ----------

display(
    spark.sql(f"DESCRIBE HISTORY {DIM_MERGE}").select(
        "version", "timestamp", "operation", "operationMetrics"
    ).limit(5)
)
