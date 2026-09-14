# Databricks notebook source
# Baixa os zips semestrais da ANP e grava o CSV extraido no volume de landing.
# O nome do arquivo carrega o hash do zip de origem, para que uma republicacao
# do mesmo semestre entre como arquivo novo na ingestao.
# O registro de auditoria e gravado a cada semestre, e nao ao final do laco:
# uma falha no meio da execucao deixaria arquivos no volume sem registro.
# Executado pelo job declarado em resources/job_bronze.yml.

# COMMAND ----------

import hashlib
import io
import os
import shutil
import zipfile
from datetime import datetime, timezone

import requests

dbutils.widgets.text("catalog", "anp")
dbutils.widgets.text(
    "semesters",
    "2023-01,2023-02,2024-01,2024-02,2025-01,2025-02,2026-01",
)

catalog = dbutils.widgets.get("catalog")
semesters = [s.strip() for s in dbutils.widgets.get("semesters").split(",") if s.strip()]

BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc/dsas/ca"
HEADERS = {"User-Agent": "Mozilla/5.0"}
VOLUME = f"/Volumes/{catalog}/bronze/landing"
TABELA_AUDITORIA = f"{catalog}.ops.landing_files"

COLUNAS_AUDITORIA = [
    "semester",
    "zip_sha256",
    "zip_bytes",
    "csv_name",
    "csv_bytes",
    "csv_lines",
    "downloaded_at",
]

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {TABELA_AUDITORIA} (
        semester       STRING,
        zip_sha256     STRING,
        zip_bytes      BIGINT,
        csv_name       STRING,
        csv_bytes      BIGINT,
        csv_lines      BIGINT,
        downloaded_at  TIMESTAMP
    )
    """
)

registrados = {
    linha.csv_name
    for linha in spark.table(TABELA_AUDITORIA).select("csv_name").distinct().collect()
}
print(f"arquivos ja registrados na auditoria: {len(registrados)}")

# COMMAND ----------


def registrar(linhas):
    """Grava o registro de auditoria de um semestre, encerrando a unidade de trabalho."""
    if not linhas:
        return
    (
        spark.createDataFrame(linhas, COLUNAS_AUDITORIA)
        .write.mode("append")
        .saveAsTable(TABELA_AUDITORIA)
    )


def processar_semestre(semester):
    """Baixa um semestre, grava os CSVs ausentes e registra o que ainda nao esta na auditoria."""
    url = f"{BASE}/ca-{semester}.zip"
    response = requests.get(url, timeout=300, headers=HEADERS)
    response.raise_for_status()

    # O portal responde 200 com HTML quando o arquivo nao existe, entao o status
    # sozinho nao serve como criterio de sucesso.
    content_type = response.headers.get("content-type", "")
    if "zip" not in content_type:
        raise ValueError(f"{semester}: content-type inesperado: {content_type}")

    payload = response.content
    zip_sha = hashlib.sha256(payload).hexdigest()
    linhas_auditoria = []

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        membros = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
        if not membros:
            raise ValueError(f"{semester}: nenhum CSV dentro do zip")

        for i, membro in enumerate(membros):
            sufixo = "" if len(membros) == 1 else f"_{i:02d}"
            csv_name = f"ca-{semester}__{zip_sha[:8]}{sufixo}.csv"
            destino = f"{VOLUME}/{csv_name}"

            if os.path.exists(destino):
                print(f"{semester}: {csv_name} ja no volume")
            else:
                with archive.open(membro) as origem, open(destino, "wb") as saida:
                    shutil.copyfileobj(origem, saida, length=8 * 1024 * 1024)
                print(f"{semester}: gravado {csv_name}")

            if csv_name in registrados:
                continue

            with open(destino, "rb") as f:
                linhas = sum(1 for _ in f)

            linhas_auditoria.append(
                (
                    semester,
                    zip_sha,
                    len(payload),
                    csv_name,
                    os.path.getsize(destino),
                    linhas,
                    datetime.now(timezone.utc),
                )
            )
            registrados.add(csv_name)
            print(f"{semester}: registrado {csv_name} ({linhas:,} linhas fisicas)")

    registrar(linhas_auditoria)
    return len(linhas_auditoria)


# COMMAND ----------

total = 0
for semester in semesters:
    total += processar_semestre(semester)

print(f"registros novos de auditoria nesta execucao: {total}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT semester, csv_name, csv_bytes, csv_lines, downloaded_at
        FROM {TABELA_AUDITORIA}
        ORDER BY semester, csv_name
        """
    )
)
