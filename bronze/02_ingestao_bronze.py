# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Ingestão Bronze
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Converter os arquivos CSV brutos armazenados no Unity Catalog Volume em tabelas Delta gerenciadas na camada Bronze.
# MAGIC
# MAGIC ## Princípios da camada Bronze
# MAGIC
# MAGIC - Preservar as colunas do arquivo de origem sem aplicar regras de negócio;
# MAGIC - Ler todas as colunas inicialmente como texto para evitar perda silenciosa de informação;
# MAGIC - Adicionar metadados técnicos de rastreabilidade;
# MAGIC - Persistir tabelas Delta no schema `main_catalog.bronze`;
# MAGIC - Não deduplicar, preencher nulos ou excluir registros nesta etapa.
# MAGIC
# MAGIC ## Origem dos arquivos
# MAGIC
# MAGIC `/Volumes/main_catalog/bronze/olist_raw/`

# COMMAND ----------

# DBTITLE 1,Configurações e importações
from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = "main_catalog"
BRONZE_SCHEMA = "bronze"
VOLUME_PATH = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/olist_raw"

PIPELINE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

print(f"Volume de origem: {VOLUME_PATH}")
print(f"Pipeline run ID: {PIPELINE_RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Mapa de arquivos e tabelas
fontes_bronze = {
    "olist_customers_dataset.csv": "bronze_customers",
    "olist_geolocation_dataset.csv": "bronze_geolocation",
    "olist_order_items_dataset.csv": "bronze_order_items",
    "olist_order_payments_dataset.csv": "bronze_order_payments",
    "olist_order_reviews_dataset.csv": "bronze_order_reviews",
    "olist_orders_dataset.csv": "bronze_orders",
    "olist_products_dataset.csv": "bronze_products",
    "olist_sellers_dataset.csv": "bronze_sellers",
    "product_category_name_translation.csv": "bronze_category_translation",
}

print(f"Total de arquivos a ingerir: {len(fontes_bronze)}")

# COMMAND ----------

# DBTITLE 1,Função de ingestão
def ingest_bronze(csv_file: str, table_name: str) -> int:
    source_path = f"{VOLUME_PATH}/{csv_file}"
    target_table = f"{CATALOG}.{BRONZE_SCHEMA}.{table_name}"

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("encoding", "UTF-8")
        .csv(source_path)
    )

    original_columns = df_raw.columns

    df_bronze = (
        df_raw
        .withColumn("source_file", F.lit(csv_file))
        .withColumn("ingestion_timestamp_utc", F.current_timestamp())
        .withColumn("pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        .withColumn(
            "record_hash",
            F.sha2(
                F.concat_ws(
                    "||",
                    *[
                        F.coalesce(F.col(column).cast("string"), F.lit("∅"))
                        for column in original_columns
                    ]
                ),
                256,
            ),
        )
    )

    (
        df_bronze.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(target_table)
    )

    row_count = spark.table(target_table).count()

    print(f"Tabela criada: {target_table} | Linhas: {row_count}")
    return row_count

# COMMAND ----------

# DBTITLE 1,Executar a ingestão
resultado_ingestao = {}

for arquivo, tabela in fontes_bronze.items():
    resultado_ingestao[tabela] = ingest_bronze(arquivo, tabela)

resultado_ingestao

# COMMAND ----------

# DBTITLE 1,Validar tabela criada
tabelas_esperadas = {
    f"{CATALOG}.{BRONZE_SCHEMA}.{tabela}"
    for tabela in fontes_bronze.values()
}

tabelas_encontradas = {
    f"{tabela.catalog}.{tabela.namespace[0]}.{tabela.name}"
    for tabela in spark.catalog.listTables(f"{CATALOG}.{BRONZE_SCHEMA}")
    if not tabela.isTemporary
}

faltantes = tabelas_esperadas - tabelas_encontradas

print(f"Tabelas esperadas: {len(tabelas_esperadas)}")
print(f"Tabelas encontradas: {len(tabelas_encontradas)}")
print(f"Tabelas faltantes: {sorted(faltantes)}")

assert not faltantes, (
    "A ingestão Bronze não criou todas as tabelas esperadas. "
    f"Tabelas ausentes: {sorted(faltantes)}"
)

print("STATUS: APROVADO — todas as tabelas Bronze foram criadas.")