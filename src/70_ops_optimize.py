# Databricks notebook source
# Mede o layout fisico e o tempo de consulta do fato antes e depois de otimizar.
# A linha de base e registrada primeiro: uma otimizacao sem medida anterior nao
# tem como ser avaliada, e o resultado fica em ops.benchmark_runs para que as
# comparacoes sobrevivam a sessao.
# A primeira execucao de cada consulta e descartada por medir partida a frio, e o
# valor usado e a mediana das repeticoes, nao a media.
# Entre a linha de base e o OPTIMIZE roda uma fase de controle, que repete as
# mesmas consultas sem alterar nada: se ela sozinha ja mostrar ganho, o efeito
# medido nas fases seguintes e aquecimento do ambiente, e nao layout.
# Executado pelo job declarado em resources/job_ops.yml.

# COMMAND ----------

import statistics
import time
from datetime import datetime, timezone

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text("repeticoes", "4")
dbutils.widgets.dropdown("aplicar_clustering", "true", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
repeticoes = int(dbutils.widgets.get("repeticoes"))
aplicar_clustering = dbutils.widgets.get("aplicar_clustering") == "true"

FACT = f"{catalog}.gold.fct_price_observation"
WEEKLY = f"{catalog}.gold.weekly_price"
DIM_STATION = f"{catalog}.silver.dim_station"
DIM_PRODUCT = f"{catalog}.gold.dim_product"
RUNS = f"{catalog}.ops.benchmark_runs"

CNPJ_EXEMPLO = "14033566000124"

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {RUNS} (
        fase            STRING,
        consulta        STRING,
        execucoes       INT,
        mediana_seg     DOUBLE,
        minimo_seg      DOUBLE,
        maximo_seg      DOUBLE,
        num_files       BIGINT,
        size_bytes      BIGINT,
        clustering_cols STRING,
        medido_em       TIMESTAMP
    )
    """
)

# COMMAND ----------

CONSULTAS = {
    "filtro_seletivo": f"""
        SELECT count(*) AS n, avg(sale_price) AS media
        FROM {FACT}
        WHERE collection_date BETWEEN DATE'2025-01-01' AND DATE'2025-03-31'
          AND product_key = (
              SELECT product_key FROM {DIM_PRODUCT} WHERE product_name = 'Gasolina comum'
          )
    """,
    "agregacao_ampla": f"""
        SELECT s.state, year(f.collection_date) AS ano,
               percentile(f.sale_price, 0.5) AS mediana
        FROM {FACT} f
        JOIN {DIM_STATION} s ON s.station_key = f.station_key
        GROUP BY ALL
    """,
    "lookup_posto": f"""
        SELECT count(*) AS n, min(f.sale_price) AS menor, max(f.sale_price) AS maior
        FROM {FACT} f
        JOIN {DIM_STATION} s ON s.station_key = f.station_key
        WHERE s.reseller_cnpj = '{CNPJ_EXEMPLO}'
    """,
}

# COMMAND ----------


def detalhe_tabela(tabela):
    """Numero de arquivos, tamanho em bytes e colunas de clustering da tabela."""
    linha = spark.sql(f"DESCRIBE DETAIL {tabela}").collect()[0]
    cols = linha["clusteringColumns"] if "clusteringColumns" in linha.asDict() else None
    return (
        linha["numFiles"],
        linha["sizeInBytes"],
        ", ".join(cols) if cols else "",
    )


def cronometra(sql):
    """Executa a consulta e devolve o tempo em segundos, forcando materializacao."""
    inicio = time.perf_counter()
    spark.sql(sql).collect()
    return time.perf_counter() - inicio


def mede_fase(fase):
    """Roda todas as consultas na fase indicada e grava o resultado."""
    num_files, size_bytes, cols = detalhe_tabela(FACT)
    registros = []

    for nome, sql in CONSULTAS.items():
        tempos = [cronometra(sql) for _ in range(repeticoes)]
        uteis = tempos[1:]  # descarta a partida a frio
        registros.append(
            (
                fase,
                nome,
                len(uteis),
                statistics.median(uteis),
                min(uteis),
                max(uteis),
                num_files,
                size_bytes,
                cols,
                datetime.now(timezone.utc),
            )
        )
        print(f"{fase} · {nome}: mediana {statistics.median(uteis):.2f}s de {tempos}")

    colunas = [
        "fase", "consulta", "execucoes", "mediana_seg", "minimo_seg", "maximo_seg",
        "num_files", "size_bytes", "clustering_cols", "medido_em",
    ]
    spark.createDataFrame(registros, colunas).write.mode("append").saveAsTable(RUNS)
    print(f"{fase}: {num_files} arquivos, {size_bytes / 1e6:.1f} MB, clustering '{cols}'")


# COMMAND ----------

# Se a otimizacao preditiva estiver ativa, a plataforma reorganiza os arquivos por
# conta propria e a comparacao entre fases deixa de isolar o efeito do OPTIMIZE.
try:
    display(
        spark.sql(
            f"""
            SELECT catalog_name, schema_name, table_name, predictive_optimization_state
            FROM system.information_schema.tables
            WHERE catalog_name = '{catalog}' AND schema_name IN ('gold', 'silver')
            """
        )
    )
except Exception as e:
    print("estado da otimizacao preditiva indisponivel:", type(e).__name__, str(e)[:200])

# COMMAND ----------

mede_fase("linha_de_base")

# COMMAND ----------

# Placebo: nada foi alterado entre esta medicao e a anterior.
mede_fase("controle_sem_mudanca")

# COMMAND ----------

t0 = time.perf_counter()
display(spark.sql(f"OPTIMIZE {FACT}"))
print(f"OPTIMIZE levou {time.perf_counter() - t0:.1f}s")

# COMMAND ----------

mede_fase("pos_optimize")

# COMMAND ----------

# Liquid clustering substitui o particionamento: as colunas sao declaradas na
# tabela e a reorganizacao acontece no OPTIMIZE seguinte.
if aplicar_clustering:
    try:
        spark.sql(f"ALTER TABLE {FACT} CLUSTER BY (collection_date, product_key)")
        t0 = time.perf_counter()
        spark.sql(f"OPTIMIZE {FACT} FULL")
        print(f"OPTIMIZE FULL levou {time.perf_counter() - t0:.1f}s")
        clustering_ok = True
    except Exception as e:
        print("clustering indisponivel:", type(e).__name__, str(e)[:400])
        clustering_ok = False
else:
    clustering_ok = False
    print("clustering nao solicitado nesta execucao")

# COMMAND ----------

if clustering_ok:
    mede_fase("pos_clustering")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT consulta, fase, round(mediana_seg, 2) AS mediana_seg,
               num_files, round(size_bytes / 1e6, 1) AS size_mb, clustering_cols
        FROM {RUNS}
        WHERE medido_em >= current_timestamp() - INTERVAL 2 HOURS
        ORDER BY consulta, medido_em
        """
    )
)

# COMMAND ----------

# Ganho relativo por consulta, em relacao a linha de base da mesma execucao.
display(
    spark.sql(
        f"""
        WITH recente AS (
            SELECT * FROM {RUNS} WHERE medido_em >= current_timestamp() - INTERVAL 2 HOURS
        ),
        base AS (
            SELECT consulta, mediana_seg AS base_seg FROM recente WHERE fase = 'linha_de_base'
        )
        SELECT
            r.consulta,
            r.fase,
            round(r.mediana_seg, 2) AS mediana_seg,
            round(b.base_seg, 2) AS base_seg,
            round(100 * (b.base_seg - r.mediana_seg) / b.base_seg, 1) AS ganho_pct
        FROM recente r JOIN base b ON b.consulta = r.consulta
        WHERE r.fase <> 'linha_de_base'
        ORDER BY r.consulta, r.fase
        """
    )
)

# COMMAND ----------

display(spark.sql(f"DESCRIBE HISTORY {FACT}"))

# COMMAND ----------

# O ganho atribuivel ao layout e o que excede o ganho ja observado no controle.
display(
    spark.sql(
        f"""
        WITH recente AS (
            SELECT * FROM {RUNS} WHERE medido_em >= current_timestamp() - INTERVAL 2 HOURS
        ),
        base AS (
            SELECT consulta, mediana_seg AS base_seg FROM recente WHERE fase = 'linha_de_base'
        ),
        controle AS (
            SELECT consulta, mediana_seg AS controle_seg
            FROM recente WHERE fase = 'controle_sem_mudanca'
        )
        SELECT
            r.consulta,
            r.fase,
            round(b.base_seg, 2)     AS base_seg,
            round(c.controle_seg, 2) AS controle_seg,
            round(r.mediana_seg, 2)  AS fase_seg,
            round(100 * (b.base_seg - c.controle_seg) / b.base_seg, 1) AS ganho_do_aquecimento_pct,
            round(100 * (c.controle_seg - r.mediana_seg) / b.base_seg, 1) AS ganho_do_layout_pct
        FROM recente r
        JOIN base b     ON b.consulta = r.consulta
        JOIN controle c ON c.consulta = r.consulta
        WHERE r.fase NOT IN ('linha_de_base', 'controle_sem_mudanca')
        ORDER BY r.consulta, r.fase
        """
    )
)
