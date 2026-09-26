# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Validação da Landing Zone
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Validar a disponibilidade dos arquivos CSV brutos do dataset Brazilian E-Commerce Public Dataset by Olist no Unity Catalog Volume do Databricks.
# MAGIC
# MAGIC ## Regra de aceitação
# MAGIC
# MAGIC A landing zone estará aprovada somente quando os nove arquivos obrigatórios estiverem disponíveis no caminho:
# MAGIC
# MAGIC `/Volumes/main_catalog/bronze/olist_raw/`
# MAGIC
# MAGIC ## Resultado esperado
# MAGIC
# MAGIC - 9 arquivos obrigatórios encontrados;
# MAGIC - nenhum arquivo obrigatório ausente;
# MAGIC - execução finalizada sem erro.

# COMMAND ----------

CATALOG = "main_catalog"
SCHEMA = "bronze"
VOLUME = "olist_raw"

VOLUME_PATH = f"/Volumes/{CATALOG}/{SCHEMA}/{VOLUME}"

arquivos_esperados = {
    "olist_customers_dataset.csv",
    "olist_geolocation_dataset.csv",
    "olist_order_items_dataset.csv",
    "olist_order_payments_dataset.csv",
    "olist_order_reviews_dataset.csv",
    "olist_orders_dataset.csv",
    "olist_products_dataset.csv",
    "olist_sellers_dataset.csv",
    "product_category_name_translation.csv",
}

arquivos_encontrados = {
    arquivo.name.rstrip("/")
    for arquivo in dbutils.fs.ls(VOLUME_PATH)
}

faltantes = arquivos_esperados - arquivos_encontrados
extras = arquivos_encontrados - arquivos_esperados

print(f"Diretório validado: {VOLUME_PATH}")
print(f"Arquivos obrigatórios esperados: {len(arquivos_esperados)}")
print(f"Arquivos encontrados no Volume: {len(arquivos_encontrados)}")
print(f"Arquivos obrigatórios faltantes: {sorted(faltantes)}")
print(f"Arquivos adicionais: {sorted(extras)}")

assert not faltantes, (
    "Validação reprovada. Arquivos obrigatórios ausentes: "
    + ", ".join(sorted(faltantes))
)

print("\nSTATUS: APROVADO")
print("Todos os arquivos obrigatórios estão disponíveis na landing zone.")