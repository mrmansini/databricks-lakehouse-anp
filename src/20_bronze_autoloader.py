# Databricks notebook source
# Le o volume de landing com Auto Loader e grava a camada bronze sem conversao.
# O schema e declarado: o cabecalho do arquivo e descartado, o que tambem descarta
# o BOM presente no CSV de origem.
# Executado pelo job declarado em resources/job_bronze.yml.

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
from pyspark.sql.types import StringType, StructField, StructType

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

LANDING = f"/Volumes/{catalog}/bronze/landing"
CHECKPOINT = f"/Volumes/{catalog}/ops/checkpoints/price_raw"
TABELA = f"{catalog}.bronze.price_raw"

# Ordem identica a do cabecalho do arquivo da ANP. Tudo texto: a conversao de
# tipo pertence a camada silver, onde a regra fica visivel e testavel.
COLUNAS = [
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

schema = StructType([StructField(nome, StringType(), True) for nome in COLUNAS])

# COMMAND ----------

leitura = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("cloudFiles.rescuedDataColumn", "_rescued_data")
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .schema(schema)
    .load(LANDING)
    .select(
        "*",
        col("_metadata.file_path").alias("source_file"),
        col("_metadata.file_modification_time").alias("source_file_modified_at"),
        current_timestamp().alias("ingested_at"),
    )
)

# COMMAND ----------

# availableNow processa o que existe hoje e encerra: e um job em lote que usa o
# controle de arquivos ja lidos do Auto Loader, sem deixar processo em execucao.
consulta = (
    leitura.writeStream.option("checkpointLocation", CHECKPOINT)
    .option("mergeSchema", "true")
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
        SELECT count(*) AS linhas_com_resgate
        FROM {TABELA}
        WHERE _rescued_data IS NOT NULL
        """
    )
)