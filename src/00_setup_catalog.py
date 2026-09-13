# Databricks notebook source
# Cria o namespace do projeto: catálogo, schemas das camadas e os volumes.
# Executado pelo job declarado em resources/job_setup.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
catalog = dbutils.widgets.get("catalog")

SCHEMAS = {
    "bronze": "Dado como veio da ANP, sem transformacao.",
    "silver": "Dado tipado, limpo e com bandeiras consolidadas.",
    "gold": "Modelo dimensional e agregados de consumo.",
    "ops": "Auditoria de carga e verificacoes de qualidade.",
}

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")

for schema, comment in SCHEMAS.items():
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema} COMMENT '{comment}'")

# Landing recebe os CSVs extraidos; checkpoints guarda o controle do Auto Loader.
# Ficam em volumes separados para que o leitor nao enxergue o proprio estado.
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.bronze.landing")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.ops.checkpoints")

# COMMAND ----------

display(spark.sql(f"SHOW SCHEMAS IN {catalog}"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.bronze"))
display(spark.sql(f"SHOW VOLUMES IN {catalog}.ops"))