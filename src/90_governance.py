# Databricks notebook source
# Documenta os metadados do projeto e aplica o controle de acesso de leitura.
# O grupo recebe acesso apenas a camada de consumo. As camadas intermediarias e a
# de auditoria ficam fora, porque quem consome o resultado nao precisa do caminho
# que levou ate ele.
# Os privilegios sao revogados antes de concedidos: rodar o notebook duas vezes
# deixa o estado igual ao declarado aqui, e nao a soma do que ja existia com o
# que foi pedido agora.
# Executado pelo job declarado em resources/job_ops.yml.

# COMMAND ----------

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text("grupo_leitura", "anp_readers")

catalog = dbutils.widgets.get("catalog")
grupo = dbutils.widgets.get("grupo_leitura")

SCHEMA_CONSUMO = "gold"
SCHEMAS_INTERNOS = ["bronze", "silver", "ops"]

# Privilegios que podem ter sido concedidos antes e precisam sair.
PRIVILEGIOS_REVOGAVEIS = ["SELECT", "MODIFY", "USE SCHEMA", "READ VOLUME", "WRITE VOLUME"]

# COMMAND ----------

# Conceder privilegio a um grupo inexistente falha no meio da execucao e deixa o
# estado pela metade. A checagem vem antes de qualquer escrita.
grupos = [linha[0] for linha in spark.sql("SHOW GROUPS").collect()]
if grupo not in grupos:
    raise ValueError(
        f"grupo '{grupo}' nao existe no workspace. "
        f"Crie em Settings, Identity and access, Groups. Grupos atuais: {grupos}"
    )

print(f"grupo '{grupo}' encontrado")

# COMMAND ----------

COMENTARIOS_TABELA = {
    f"{catalog}.bronze.price_raw":
        "Pesquisa semanal de precos da ANP como veio do arquivo, todas as colunas em texto. "
        "Inclui linhas em branco publicadas pela fonte.",
    f"{catalog}.silver.price_clean":
        "Mesma coleta com tipos convertidos e texto normalizado. "
        "Linhas em branco removidas, repeticoes ainda presentes.",
    f"{catalog}.silver.price_observation":
        "Uma linha por data de coleta, posto e produto. Base das camadas de consumo.",
    f"{catalog}.silver.dim_station":
        "Historico de cada posto com vigencia [valid_from, valid_to). "
        "A versao vigente termina em 9999-12-31.",
    f"{catalog}.silver.brand_alias":
        "Rotulos que designam a mesma distribuidora. Lista escrita a mao: a fonte troca o "
        "rotulo posto a posto ao longo de anos, entao nenhuma regra automatica separa "
        "renomeacao de troca real.",
    f"{catalog}.gold.fct_price_observation":
        "Preco praticado por data, posto e produto. Bandeira e municipio vem da versao "
        "vigente do posto, e nao do fato.",
    f"{catalog}.gold.weekly_price":
        "Resumo semanal por municipio e produto, com percentis exatos. "
        "As semanas do comeco e do fim da serie ficam marcadas em is_edge_week.",
    f"{catalog}.ops.landing_files":
        "Um registro por arquivo gravado no volume, com hash do zip de origem.",
    f"{catalog}.ops.benchmark_runs":
        "Tempos medidos por execucao, identificados em run_id.",
}

for tabela, texto in COMENTARIOS_TABELA.items():
    try:
        spark.sql(f"COMMENT ON TABLE {tabela} IS '{texto}'")
    except Exception as e:
        print(f"{tabela}: {type(e).__name__} {str(e)[:150]}")

print(f"{len(COMENTARIOS_TABELA)} tabelas comentadas")

# COMMAND ----------

COMENTARIOS_COLUNA = [
    (f"{catalog}.silver.dim_station", "attribute_hash",
     "Resumo dos atributos que definem a versao. O complemento de endereco fica fora "
     "de proposito: e texto livre e instavel na fonte."),
    (f"{catalog}.silver.dim_station", "valid_to",
     "Exclusivo. A versao vigente usa 9999-12-31 em vez de um valor infinito, porque "
     "infinito nao tem representacao em DATE fora do PostgreSQL."),
    (f"{catalog}.silver.dim_station", "station_key",
     "Derivada do CNPJ com o inicio da vigencia. A mesma entrada gera a mesma chave em "
     "qualquer execucao, o que permite comparar bases diferentes."),
    (f"{catalog}.gold.weekly_price", "is_edge_week",
     "Marca a primeira e a ultima semana da serie, truncadas pela forma como a coleta "
     "comeca e termina. Ficam na tabela para que o corte seja decisao de quem consome."),
    (f"{catalog}.gold.weekly_price", "median_price",
     "Percentil exato, e nao aproximado, para poder ser comparado com outra implementacao."),
    (f"{catalog}.gold.dim_city", "city_key",
     "Derivada do par UF e nome. O nome sozinho nao e chave: ha municipio homonimo em "
     "UFs diferentes."),
]

for tabela, coluna, texto in COMENTARIOS_COLUNA:
    try:
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{texto}'")
    except Exception as e:
        print(f"{tabela}.{coluna}: {type(e).__name__} {str(e)[:150]}")

print(f"{len(COMENTARIOS_COLUNA)} colunas comentadas")

# COMMAND ----------

def revoga(privilegio, objeto, tipo):
    """Remove um privilegio, ignorando o caso de ele nao existir."""
    try:
        spark.sql(f"REVOKE {privilegio} ON {tipo} {objeto} FROM `{grupo}`")
    except Exception:
        pass


for schema in SCHEMAS_INTERNOS + [SCHEMA_CONSUMO]:
    for privilegio in PRIVILEGIOS_REVOGAVEIS:
        revoga(privilegio, f"{catalog}.{schema}", "SCHEMA")

revoga("USE CATALOG", catalog, "CATALOG")
revoga("SELECT", catalog, "CATALOG")

print("privilegios anteriores removidos")

# COMMAND ----------

# Leitura chega ao dado por tres niveis: entrar no catalogo, entrar no schema e ler
# as tabelas. Falta de qualquer um deles bloqueia o acesso.
spark.sql(f"GRANT USE CATALOG ON CATALOG {catalog} TO `{grupo}`")
spark.sql(f"GRANT USE SCHEMA ON SCHEMA {catalog}.{SCHEMA_CONSUMO} TO `{grupo}`")
spark.sql(f"GRANT SELECT ON SCHEMA {catalog}.{SCHEMA_CONSUMO} TO `{grupo}`")

print(f"'{grupo}' recebeu leitura em {catalog}.{SCHEMA_CONSUMO}")

# COMMAND ----------

display(spark.sql(f"SHOW GRANTS `{grupo}` ON CATALOG {catalog}"))

# COMMAND ----------

for schema in [SCHEMA_CONSUMO] + SCHEMAS_INTERNOS:
    print(f"\n{catalog}.{schema}:")
    display(spark.sql(f"SHOW GRANTS `{grupo}` ON SCHEMA {catalog}.{schema}"))

# COMMAND ----------

# Auditoria pelo catalogo de sistema: mostra o estado efetivo, e nao a sequencia de
# comandos que foi executada.
display(
    spark.sql(
        f"""
        SELECT grantee, table_schema, table_name, privilege_type
        FROM {catalog}.information_schema.table_privileges
        WHERE grantee = '{grupo}'
        ORDER BY table_schema, table_name
        """
    )
)

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT grantee, schema_name, privilege_type
        FROM {catalog}.information_schema.schema_privileges
        WHERE grantee = '{grupo}'
        ORDER BY schema_name
        """
    )
)

# COMMAND ----------

# O resultado esperado: privilegio apenas no schema de consumo.
schemas_com_acesso = [
    linha[0]
    for linha in spark.sql(
        f"""
        SELECT DISTINCT schema_name
        FROM {catalog}.information_schema.schema_privileges
        WHERE grantee = '{grupo}'
        """
    ).collect()
]

indevidos = [s for s in schemas_com_acesso if s in SCHEMAS_INTERNOS]

print(f"schemas com privilegio para '{grupo}': {schemas_com_acesso}")
print(f"schemas internos indevidamente acessiveis: {indevidos}")

if indevidos:
    raise ValueError(f"grupo de leitura tem acesso a schema interno: {indevidos}")

if SCHEMA_CONSUMO not in schemas_com_acesso:
    raise ValueError(f"grupo de leitura nao tem acesso a {SCHEMA_CONSUMO}")

print("estado de acesso conforme o declarado")
