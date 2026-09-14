# Databricks notebook source
# Le o volume de landing com Auto Loader e grava a camada bronze sem conversao.
# Os nomes de coluna vem do cabecalho do arquivo e sao validados contra o layout
# esperado antes da renomeacao posicional: o Auto Loader casa colunas por nome,
# entao declarar nomes proprios faria todo o conteudo cair na coluna de resgate.
# A renomeacao posicional tambem descarta o BOM presente no nome da primeira coluna.
# A comparacao dobra acento e caixa: o leitor remove os acentos dos nomes de
# coluna ao fixar o schema, entao a grafia do cabecalho nao serve como contrato.
# O que a validacao garante e a identidade e a ordem das colunas.
# Executado pelo job declarado em resources/job_bronze.yml.

# COMMAND ----------

import unicodedata

from pyspark.sql.functions import col, current_timestamp

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

catalog = dbutils.widgets.get("catalog")
full_refresh = dbutils.widgets.get("full_refresh") == "true"

LANDING = f"/Volumes/{catalog}/bronze/landing"
CHECKPOINT = f"/Volumes/{catalog}/ops/checkpoints/price_raw"
SCHEMA_LOCATION = f"/Volumes/{catalog}/ops/checkpoints/price_raw_schema"
TABELA = f"{catalog}.bronze.price_raw"

# Layout publicado pela ANP, na ordem em que aparece no cabecalho.
LAYOUT_ESPERADO = [
    "Regiao - Sigla",
    "Estado - Sigla",
    "Município",
    "Revenda",
    "CNPJ da Revenda",
    "Nome da Rua",
    "Numero Rua",
    "Complemento",
    "Bairro",
    "Cep",
    "Produto",
    "Data da Coleta",
    "Valor de Venda",
    "Valor de Compra",
    "Unidade de Medida",
    "Bandeira",
]

# Nomes de destino, na mesma ordem. Tudo texto: a conversao de tipo pertence a
# camada silver, onde a regra fica visivel e testavel.
COLUNAS_DESTINO = [
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


def chave(nome):
    """Reduz um nome de coluna a uma chave comparavel: sem BOM, sem acento, minusculo."""
    limpo = unicodedata.normalize("NFKD", nome.replace("\ufeff", "").strip())
    return "".join(ch for ch in limpo if not unicodedata.combining(ch)).lower()


# COMMAND ----------

if full_refresh:
    spark.sql(f"DROP TABLE IF EXISTS {TABELA}")
    dbutils.fs.rm(CHECKPOINT, True)
    dbutils.fs.rm(SCHEMA_LOCATION, True)
    print("full refresh: tabela, checkpoint e schema removidos")

# COMMAND ----------

leitura = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.schemaLocation", SCHEMA_LOCATION)
    .option("cloudFiles.inferColumnTypes", "false")
    .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .load(LANDING)
)

# COMMAND ----------

origem = [c for c in leitura.columns if not c.startswith("_")]
lido = [chave(c) for c in origem]
esperado = [chave(c) for c in LAYOUT_ESPERADO]

if lido != esperado:
    if len(lido) != len(esperado):
        detalhe = f"contagem divergente: {len(lido)} colunas lidas, {len(esperado)} esperadas"
    else:
        detalhe = "\n".join(
            f"posicao {i}: lido '{a}' != esperado '{b}'"
            for i, (a, b) in enumerate(zip(lido, esperado))
            if a != b
        )
    raise ValueError(
        "layout divergente do esperado.\n"
        f"lido:     {lido}\n"
        f"esperado: {esperado}\n"
        f"{detalhe}"
    )

print(f"layout validado: {len(origem)} colunas na ordem esperada")

# COMMAND ----------

projecao = [
    leitura[nome].alias(destino) for nome, destino in zip(origem, COLUNAS_DESTINO)
] + [
    col("_metadata.file_path").alias("source_file"),
    col("_metadata.file_modification_time").alias("source_file_modified_at"),
    current_timestamp().alias("ingested_at"),
]

# COMMAND ----------

# availableNow processa o que existe hoje e encerra: e um job em lote que usa o
# controle de arquivos ja lidos do Auto Loader, sem deixar processo em execucao.
consulta = (
    leitura.select(*projecao)
    .writeStream.option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)
    .toTable(TABELA)
)
consulta.awaitTermination()

print("lote encerrado")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT
            regexp_extract(source_file, '(ca-[0-9]{{4}}-[0-9]{{2}}__[0-9a-f]{{8}}[^/]*)', 1) AS csv_name,
            count(*) AS linhas_bronze
        FROM {TABELA}
        GROUP BY ALL
        ORDER BY csv_name
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT count(*) AS linhas_com_coluna_nula
        FROM {TABELA}
        WHERE region IS NULL OR product IS NULL OR sale_price IS NULL
        """
    )
)
