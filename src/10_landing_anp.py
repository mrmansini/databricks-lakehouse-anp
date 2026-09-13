# Databricks notebook source
# Baixa os zips semestrais da ANP e grava o CSV extraido no volume de landing.
# O nome do arquivo carrega o hash do zip de origem, para que uma republicacao
# do mesmo semestre entre como arquivo novo na ingestao.
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

# COMMAND ----------

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog}.ops.landing_files (
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

# COMMAND ----------


def fetch_semester(semester):
    """Baixa um semestre e grava cada CSV do zip no volume. Devolve os registros gravados."""
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
    registros = []

    with zipfile.ZipFile(io.BytesIO(payload)) as archive:
        membros = [m for m in archive.infolist() if m.filename.lower().endswith(".csv")]
        if not membros:
            raise ValueError(f"{semester}: nenhum CSV dentro do zip")

        for i, membro in enumerate(membros):
            sufixo = "" if len(membros) == 1 else f"_{i:02d}"
            csv_name = f"ca-{semester}__{zip_sha[:8]}{sufixo}.csv"
            destino = f"{VOLUME}/{csv_name}"

            if os.path.exists(destino):
                print(f"{semester}: ja presente como {csv_name}")
                continue

            with archive.open(membro) as origem, open(destino, "wb") as saida:
                shutil.copyfileobj(origem, saida, length=8 * 1024 * 1024)

            with open(destino, "rb") as f:
                linhas = sum(1 for _ in f)

            registros.append(
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
            print(f"{semester}: gravado {csv_name} ({linhas:,} linhas fisicas)")

    return registros


# COMMAND ----------

novos = []
for semester in semesters:
    novos.extend(fetch_semester(semester))

if novos:
    colunas = [
        "semester",
        "zip_sha256",
        "zip_bytes",
        "csv_name",
        "csv_bytes",
        "csv_lines",
        "downloaded_at",
    ]
    (
        spark.createDataFrame(novos, colunas)
        .write.mode("append")
        .saveAsTable(f"{catalog}.ops.landing_files")
    )

print(f"arquivos novos nesta execucao: {len(novos)}")

# COMMAND ----------

display(
    spark.sql(
        f"""
        SELECT semester, csv_name, csv_bytes, csv_lines, downloaded_at
        FROM {catalog}.ops.landing_files
        ORDER BY semester, csv_name
        """
    )
)