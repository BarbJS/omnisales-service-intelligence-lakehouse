# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Transformação e Qualidade da Camada Silver
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Transformar as tabelas Delta da camada Bronze em tabelas Silver tipadas, padronizadas, deduplicadas e validadas.
# MAGIC
# MAGIC ## Princípios aplicados
# MAGIC
# MAGIC - Conversão explícita de tipos de dados;
# MAGIC - Padronização de atributos textuais;
# MAGIC - Tratamento controlado de campos nulos;
# MAGIC - Deduplicação por chaves de negócio;
# MAGIC - Preservação de registros inválidos em tabela de quarentena;
# MAGIC - Agregação de entidades com múltiplos registros por pedido;
# MAGIC - Criação de atributos logísticos derivados;
# MAGIC - Persistência no schema `main_catalog.silver`.
# MAGIC
# MAGIC ## Escopo
# MAGIC
# MAGIC A camada Silver prepara os dados para a modelagem Gold e para a análise de confiabilidade logística e satisfação do cliente. Nenhuma tabela Gold será criada neste notebook.

# COMMAND ----------

# DBTITLE 1,Configuração e importações
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DecimalType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    TimestampType,
)

CATALOG = "main_catalog"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

BRONZE = f"{CATALOG}.{BRONZE_SCHEMA}"
SILVER = f"{CATALOG}.{SILVER_SCHEMA}"

PIPELINE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

print(f"Origem Bronze: {BRONZE}")
print(f"Destino Silver: {SILVER}")
print(f"Pipeline run ID: {PIPELINE_RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Funções reutilizáveis
def normalize_text(column_name: str):
    return F.lower(
        F.trim(
            F.regexp_replace(
                F.coalesce(F.col(column_name).cast("string"), F.lit("")),
                r"\s+",
                " "
            )
        )
    )

def normalize_city(column_name: str):
    return F.initcap(normalize_text(column_name))

def write_silver(df: DataFrame, table_name: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{SILVER}.{table_name}")
    )
    print(f"Tabela Silver criada: {SILVER}.{table_name} | Linhas: {df.count()}")

def with_silver_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("silver_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        .withColumn("silver_processed_timestamp_utc", F.current_timestamp())
    )

# COMMAND ----------

# DBTITLE 1,Transformar clientes
customers_bronze = spark.table(f"{BRONZE}.bronze_customers")

customers_silver = (
    customers_bronze
    .select(
        F.trim("customer_id").alias("customer_id"),
        F.trim("customer_unique_id").alias("customer_unique_id"),
        F.lpad(
            F.regexp_replace(F.trim("customer_zip_code_prefix"), r"\.0$", ""),
            5,
            "0"
        ).alias("customer_zip_code_prefix"),
        normalize_city("customer_city").alias("customer_city"),
        F.upper(F.trim("customer_state")).alias("customer_state"),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
    .filter(F.col("customer_id").isNotNull() & (F.col("customer_id") != ""))
    .dropDuplicates(["customer_id"])
)

customers_silver = with_silver_metadata(customers_silver)

write_silver(customers_silver, "silver_customers")

# COMMAND ----------

# DBTITLE 1,Transformar vendedores
sellers_bronze = spark.table(f"{BRONZE}.bronze_sellers")

sellers_silver = (
    sellers_bronze
    .select(
        F.trim("seller_id").alias("seller_id"),
        F.lpad(
            F.regexp_replace(F.trim("seller_zip_code_prefix"), r"\.0$", ""),
            5,
            "0"
        ).alias("seller_zip_code_prefix"),
        normalize_city("seller_city").alias("seller_city"),
        F.upper(F.trim("seller_state")).alias("seller_state"),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
    .filter(F.col("seller_id").isNotNull() & (F.col("seller_id") != ""))
    .dropDuplicates(["seller_id"])
)

sellers_silver = with_silver_metadata(sellers_silver)

write_silver(sellers_silver, "silver_sellers")

# COMMAND ----------

# DBTITLE 1,Transformar tradução de categoria
category_translation_bronze = spark.table(
    f"{BRONZE}.bronze_category_translation"
)

category_translation_silver = (
    category_translation_bronze
    .select(
        normalize_text("product_category_name").alias("product_category_name"),
        normalize_text("product_category_name_english").alias(
            "product_category_name_english"
        ),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
    .filter(
        F.col("product_category_name").isNotNull()
        & (F.col("product_category_name") != "")
    )
    .dropDuplicates(["product_category_name"])
)

category_translation_silver = with_silver_metadata(category_translation_silver)

write_silver(category_translation_silver, "silver_category_translation")

# COMMAND ----------

# DBTITLE 1,Transformar produtos
products_bronze = spark.table(f"{BRONZE}.bronze_products")

products_silver = (
    products_bronze
    .select(
        F.trim("product_id").alias("product_id"),
        normalize_text("product_category_name").alias("product_category_name"),
        F.col("product_name_lenght").cast(IntegerType()).alias(
            "product_name_length"
        ),
        F.col("product_description_lenght").cast(IntegerType()).alias(
            "product_description_length"
        ),
        F.col("product_photos_qty").cast(IntegerType()).alias(
            "product_photos_qty"
        ),
        F.col("product_weight_g").cast(DoubleType()).alias("product_weight_g"),
        F.col("product_length_cm").cast(DoubleType()).alias(
            "product_length_cm"
        ),
        F.col("product_height_cm").cast(DoubleType()).alias(
            "product_height_cm"
        ),
        F.col("product_width_cm").cast(DoubleType()).alias("product_width_cm"),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
    .filter(F.col("product_id").isNotNull() & (F.col("product_id") != ""))
    .dropDuplicates(["product_id"])
)

products_silver = with_silver_metadata(products_silver)

write_silver(products_silver, "silver_products")

# COMMAND ----------

# DBTITLE 1,Transformar pedidos e gerar quarentena
orders_bronze = spark.table(f"{BRONZE}.bronze_orders")

orders_typed = (
    orders_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.trim("customer_id").alias("customer_id"),
        normalize_text("order_status").alias("order_status"),
        F.to_timestamp("order_purchase_timestamp").alias(
            "order_purchase_timestamp"
        ),
        F.to_timestamp("order_approved_at").alias("order_approved_at"),
        F.to_timestamp("order_delivered_carrier_date").alias(
            "order_delivered_carrier_date"
        ),
        F.to_timestamp("order_delivered_customer_date").alias(
            "order_delivered_customer_date"
        ),
        F.to_timestamp("order_estimated_delivery_date").alias(
            "order_estimated_delivery_date"
        ),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
)

orders_quarantine = (
    orders_typed
    .filter(
        F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | F.col("customer_id").isNull()
        | (F.col("customer_id") == "")
        | F.col("order_purchase_timestamp").isNull()
        | (~F.col("order_status").isin(
            "created",
            "approved",
            "invoiced",
            "processing",
            "shipped",
            "delivered",
            "unavailable",
            "canceled",
        ))
    )
    .withColumn(
        "quarantine_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_id").isNull() | (F.col("order_id") == ""),
                F.lit("order_id ausente"),
            ),
            F.when(
                F.col("customer_id").isNull() | (F.col("customer_id") == ""),
                F.lit("customer_id ausente"),
            ),
            F.when(
                F.col("order_purchase_timestamp").isNull(),
                F.lit("data de compra inválida ou ausente"),
            ),
            F.when(
                ~F.col("order_status").isin(
                    "created",
                    "approved",
                    "invoiced",
                    "processing",
                    "shipped",
                    "delivered",
                    "unavailable",
                    "canceled",
                ),
                F.lit("status de pedido inválido"),
            ),
        )
    )
)

orders_valid = (
    orders_typed
    .join(
        orders_quarantine.select("record_hash").distinct(),
        on="record_hash",
        how="left_anti",
    )
    .dropDuplicates(["order_id"])
)

write_silver(with_silver_metadata(orders_quarantine), "silver_quarantine_orders")
write_silver(with_silver_metadata(orders_valid), "silver_orders")

# COMMAND ----------

# DBTITLE 1,Transformar itens de pedido e gerar quarentena
items_bronze = spark.table(f"{BRONZE}.bronze_order_items")

items_typed = (
    items_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("order_item_id").cast(IntegerType()).alias("order_item_id"),
        F.trim("product_id").alias("product_id"),
        F.trim("seller_id").alias("seller_id"),
        F.to_timestamp("shipping_limit_date").alias("shipping_limit_date"),
        F.col("price").cast(DecimalType(18, 2)).alias("price"),
        F.col("freight_value").cast(DecimalType(18, 2)).alias(
            "freight_value"
        ),
        "source_file",
        "ingestion_timestamp_utc",
        "pipeline_run_id",
        "record_hash",
    )
)

items_quarantine = (
    items_typed
    .filter(
        F.col("order_id").isNull()
        | (F.col("order_id") == "")
        | F.col("order_item_id").isNull()
        | F.col("product_id").isNull()
        | (F.col("product_id") == "")
        | F.col("seller_id").isNull()
        | (F.col("seller_id") == "")
        | F.col("price").isNull()
        | (F.col("price") < 0)
        | F.col("freight_value").isNull()
        | (F.col("freight_value") < 0)
    )
    .withColumn(
        "quarantine_reason",
        F.concat_ws(
            "; ",
            F.when(
                F.col("order_id").isNull() | (F.col("order_id") == ""),
                F.lit("order_id ausente"),
            ),
            F.when(
                F.col("order_item_id").isNull(),
                F.lit("order_item_id inválido"),
            ),
            F.when(
                F.col("product_id").isNull() | (F.col("product_id") == ""),
                F.lit("product_id ausente"),
            ),
            F.when(
                F.col("seller_id").isNull() | (F.col("seller_id") == ""),
                F.lit("seller_id ausente"),
            ),
            F.when(
                F.col("price").isNull() | (F.col("price") < 0),
                F.lit("preço inválido"),
            ),
            F.when(
                F.col("freight_value").isNull()
                | (F.col("freight_value") < 0),
                F.lit("frete inválido"),
            ),
        )
    )
)

items_valid = (
    items_typed
    .join(
        items_quarantine.select("record_hash").distinct(),
        on="record_hash",
        how="left_anti",
    )
    .dropDuplicates(["order_id", "order_item_id"])
)

write_silver(with_silver_metadata(items_quarantine), "silver_quarantine_order_items")
write_silver(with_silver_metadata(items_valid), "silver_order_items")

# COMMAND ----------

# DBTITLE 1,Transformar e agregar pagamentos por pedido
payments_bronze = spark.table(f"{BRONZE}.bronze_order_payments")

payments_typed = (
    payments_bronze
    .select(
        F.trim("order_id").alias("order_id"),
        F.col("payment_sequential").cast(IntegerType()).alias(
            "payment_sequential"
        ),
        normalize_text("payment_type").alias("payment_type"),
        F.col("payment_installments").cast(IntegerType()).alias(
            "payment_installments"
        ),
        F.col("payment_value").cast(DecimalType(18, 2)).alias(
            "payment_value"
        ),
        "record_hash",
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
    .filter(F.col("payment_value").isNotNull() & (F.col("payment_value") >= 0))
    .dropDuplicates(["order_id", "payment_sequential"])
)

payments_order_agg = (
    payments_typed
    .groupBy("order_id")
    .agg(
        F.sum("payment_value").alias("payment_total_value"),
        F.count("*").alias("payment_record_count"),
        F.max("payment_installments").alias("max_payment_installments"),
        F.concat_ws(
            "|",
            F.sort_array(F.collect_set("payment_type"))
        ).alias("payment_types"),
    )
)

write_silver(
    with_silver_metadata(payments_order_agg),
    "silver_payments_order_agg",
)

# COMMAND ----------

# DBTITLE 1,Transformar e agregar avaliações por pedido
reviews_bronze = spark.table(f"{BRONZE}.bronze_order_reviews")

reviews_typed = (
    reviews_bronze
    .select(
        F.trim(F.col("review_id")).alias("review_id"),
        F.trim(F.col("order_id")).alias("order_id"),
        F.expr("try_cast(review_score as int)").alias("review_score"),
        F.trim(F.col("review_comment_title")).alias("review_comment_title"),
        F.trim(F.col("review_comment_message")).alias("review_comment_message"),
        F.expr(
            "try_to_timestamp(review_creation_date, 'yyyy-MM-dd HH:mm:ss')"
        ).alias("review_creation_date"),
        F.expr(
            "try_to_timestamp(review_answer_timestamp, 'yyyy-MM-dd HH:mm:ss')"
        ).alias("review_answer_timestamp"),
        F.col("record_hash"),
    )
    .filter(F.col("order_id").isNotNull() & (F.col("order_id") != ""))
    .filter(F.col("review_score").between(1, 5))
    .dropDuplicates(["review_id"])
)

reviews_order_agg = (
    reviews_typed
    .groupBy(F.col("order_id"))
    .agg(
        F.round(F.avg(F.col("review_score")), 2).alias("review_score_avg"),
        F.min(F.col("review_score")).alias("review_score_min"),
        F.max(F.col("review_score")).alias("review_score_max"),
        F.count(F.lit(1)).alias("review_count"),
        F.max(F.col("review_creation_date")).alias(
            "latest_review_creation_date"
        ),
    )
)

write_silver(
    with_silver_metadata(reviews_order_agg),
    "silver_reviews_order_agg",
)

# COMMAND ----------

# DBTITLE 1,Transformar e agregar geolocalização por CEP
geolocation_bronze = spark.table(f"{BRONZE}.bronze_geolocation")

geolocation_typed = (
    geolocation_bronze
    .select(
        F.lpad(
            F.regexp_replace(
                F.trim("geolocation_zip_code_prefix"),
                r"\.0$",
                "",
            ),
            5,
            "0",
        ).alias("zip_code_prefix"),
        F.col("geolocation_lat").cast(DoubleType()).alias("latitude"),
        F.col("geolocation_lng").cast(DoubleType()).alias("longitude"),
        normalize_city("geolocation_city").alias("geolocation_city"),
        F.upper(F.trim("geolocation_state")).alias("geolocation_state"),
    )
    .filter(F.col("zip_code_prefix").isNotNull() & (F.col("zip_code_prefix") != ""))
    .filter(F.col("latitude").between(-35.0, 6.0))
    .filter(F.col("longitude").between(-75.0, -30.0))
)

geolocation_zip_agg = (
    geolocation_typed
    .groupBy("zip_code_prefix")
    .agg(
        F.round(F.avg("latitude"), 6).alias("latitude"),
        F.round(F.avg("longitude"), 6).alias("longitude"),
        F.first("geolocation_city", ignorenulls=True).alias(
            "geolocation_city"
        ),
        F.first("geolocation_state", ignorenulls=True).alias(
            "geolocation_state"
        ),
        F.count("*").alias("geolocation_record_count"),
    )
)

write_silver(
    with_silver_metadata(geolocation_zip_agg),
    "silver_geolocation_zip_agg",
)

# COMMAND ----------

# DBTITLE 1,Criar tabela operacional de serviço por pedido
orders = spark.table(f"{SILVER}.silver_orders")
items = spark.table(f"{SILVER}.silver_order_items")
payments = spark.table(f"{SILVER}.silver_payments_order_agg")
reviews = spark.table(f"{SILVER}.silver_reviews_order_agg")
customers = spark.table(f"{SILVER}.silver_customers")

order_items_agg = (
    items
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("product_total_value"),
        F.sum("freight_value").alias("freight_total_value"),
        F.sum(F.col("price") + F.col("freight_value")).alias("gmv_total_value"),
        F.count("*").alias("order_item_count"),
        F.countDistinct("product_id").alias("distinct_product_count"),
        F.countDistinct("seller_id").alias("distinct_seller_count"),
        F.round(
            F.avg(
                F.when(
                    F.col("price") > 0,
                    F.col("freight_value") / F.col("price"),
                )
            ),
            4,
        ).alias("avg_freight_to_price_ratio"),
    )
)

order_service = (
    orders.alias("o")
    .join(
        customers.alias("c"),
        F.col("o.customer_id") == F.col("c.customer_id"),
        "left",
    )
    .join(
        order_items_agg.alias("i"),
        F.col("o.order_id") == F.col("i.order_id"),
        "left",
    )
    .join(
        payments.alias("p"),
        F.col("o.order_id") == F.col("p.order_id"),
        "left",
    )
    .join(
        reviews.alias("r"),
        F.col("o.order_id") == F.col("r.order_id"),
        "left",
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("c.customer_unique_id"),
        F.col("c.customer_zip_code_prefix"),
        F.col("c.customer_city"),
        F.col("c.customer_state"),
        F.col("o.order_status"),
        F.col("o.order_purchase_timestamp"),
        F.col("o.order_approved_at"),
        F.col("o.order_delivered_carrier_date"),
        F.col("o.order_delivered_customer_date"),
        F.col("o.order_estimated_delivery_date"),
        F.col("i.product_total_value"),
        F.col("i.freight_total_value"),
        F.col("i.gmv_total_value"),
        F.col("i.order_item_count"),
        F.col("i.distinct_product_count"),
        F.col("i.distinct_seller_count"),
        F.col("i.avg_freight_to_price_ratio"),
        F.col("p.payment_total_value"),
        F.col("p.payment_record_count"),
        F.col("p.max_payment_installments"),
        F.col("p.payment_types"),
        F.col("r.review_score_avg"),
        F.col("r.review_score_min"),
        F.col("r.review_score_max"),
        F.col("r.review_count"),
        F.col("r.latest_review_creation_date"),
    )
    .withColumn(
        "actual_delivery_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull(),
            F.round(
                (
                    F.unix_timestamp("order_delivered_customer_date")
                    - F.unix_timestamp("order_purchase_timestamp")
                )
                / F.lit(86400.0),
                2,
            ),
        ),
    )
    .withColumn(
        "delivery_delay_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull(),
            F.greatest(
                F.lit(0.0),
                F.round(
                    (
                        F.unix_timestamp("order_delivered_customer_date")
                        - F.unix_timestamp("order_estimated_delivery_date")
                    )
                    / F.lit(86400.0),
                    2,
                ),
            ),
        ),
    )
    .withColumn(
        "delivery_early_days",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull(),
            F.greatest(
                F.lit(0.0),
                F.round(
                    (
                        F.unix_timestamp("order_estimated_delivery_date")
                        - F.unix_timestamp("order_delivered_customer_date")
                    )
                    / F.lit(86400.0),
                    2,
                ),
            ),
        ),
    )
    .withColumn(
        "is_delivered",
        F.col("order_status") == F.lit("delivered"),
    )
    .withColumn(
        "is_delivery_late",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull(),
            F.col("order_delivered_customer_date")
            > F.col("order_estimated_delivery_date"),
        ).otherwise(F.lit(None).cast("boolean")),
    )
    .withColumn(
        "is_delivery_on_time",
        F.when(
            F.col("order_delivered_customer_date").isNotNull()
            & F.col("order_estimated_delivery_date").isNotNull(),
            F.col("order_delivered_customer_date")
            <= F.col("order_estimated_delivery_date"),
        ).otherwise(F.lit(None).cast("boolean")),
    )
    .withColumn(
        "is_low_review",
        F.when(
            F.col("review_score_avg").isNotNull(),
            F.col("review_score_avg") <= F.lit(2.0),
        ).otherwise(F.lit(None).cast("boolean")),
    )
    .withColumn(
        "sales_channel",
        F.lit("marketplace_web"),
    )
)

write_silver(
    with_silver_metadata(order_service),
    "silver_order_service_base",
)

# COMMAND ----------

# DBTITLE 1,Distância aproximada por pedido
order_service_base = spark.table(f"{SILVER}.silver_order_service_base")
order_items = spark.table(f"{SILVER}.silver_order_items")
sellers = spark.table(f"{SILVER}.silver_sellers")
geolocation = spark.table(f"{SILVER}.silver_geolocation_zip_agg")

order_seller = (
    order_items
    .groupBy("order_id")
    .agg(
        F.first(F.col("seller_id"), ignorenulls=True).alias(
            "primary_seller_id"
        ),
        F.countDistinct(F.col("seller_id")).alias("seller_count"),
    )
)

seller_origin = (
    sellers.alias("s")
    .join(
        geolocation.alias("g"),
        F.col("s.seller_zip_code_prefix") == F.col("g.zip_code_prefix"),
        "left",
    )
    .select(
        F.col("s.seller_id").alias("primary_seller_id"),
        F.col("s.seller_zip_code_prefix").alias("seller_zip_code_prefix"),
        F.col("s.seller_city").alias("seller_city"),
        F.col("s.seller_state").alias("seller_state"),
        F.col("g.latitude").alias("seller_latitude_geo"),
        F.col("g.longitude").alias("seller_longitude_geo"),
    )
)

customer_destination = (
    geolocation
    .select(
        F.col("zip_code_prefix").alias("customer_zip_code_prefix_geo"),
        F.col("latitude").alias("customer_latitude_geo"),
        F.col("longitude").alias("customer_longitude_geo"),
    )
)

order_service_enriched = (
    order_service_base.alias("o")
    .join(
        order_seller.alias("os"),
        F.col("o.order_id") == F.col("os.order_id"),
        "left",
    )
    .join(
        seller_origin.alias("so"),
        F.col("os.primary_seller_id") == F.col("so.primary_seller_id"),
        "left",
    )
    .join(
        customer_destination.alias("cd"),
        F.col("o.customer_zip_code_prefix")
        == F.col("cd.customer_zip_code_prefix_geo"),
        "left",
    )
    .select(
        F.col("o.*"),
        F.col("os.primary_seller_id"),
        F.col("os.seller_count"),
        F.col("so.seller_zip_code_prefix"),
        F.col("so.seller_city"),
        F.col("so.seller_state"),
        F.col("so.seller_latitude_geo"),
        F.col("so.seller_longitude_geo"),
        F.col("cd.customer_latitude_geo"),
        F.col("cd.customer_longitude_geo"),
    )
    .withColumn(
        "seller_customer_distance_km",
        F.when(
            F.col("seller_latitude_geo").isNotNull()
            & F.col("seller_longitude_geo").isNotNull()
            & F.col("customer_latitude_geo").isNotNull()
            & F.col("customer_longitude_geo").isNotNull(),
            F.round(
                F.lit(6371.0)
                * F.lit(2.0)
                * F.asin(
                    F.sqrt(
                        F.pow(
                            F.sin(
                                (
                                    F.radians(F.col("customer_latitude_geo"))
                                    - F.radians(F.col("seller_latitude_geo"))
                                )
                                / F.lit(2.0)
                            ),
                            F.lit(2.0),
                        )
                        + F.cos(F.radians(F.col("seller_latitude_geo")))
                        * F.cos(F.radians(F.col("customer_latitude_geo")))
                        * F.pow(
                            F.sin(
                                (
                                    F.radians(F.col("customer_longitude_geo"))
                                    - F.radians(F.col("seller_longitude_geo"))
                                )
                                / F.lit(2.0)
                            ),
                            F.lit(2.0),
                        )
                    )
                ),
                2,
            ),
        ),
    )
    .withColumn(
        "distance_band",
        F.when(
            F.col("seller_customer_distance_km").isNull(),
            F.lit("not_available"),
        )
        .when(
            F.col("seller_customer_distance_km") < 100,
            F.lit("under_100_km"),
        )
        .when(
            F.col("seller_customer_distance_km") < 500,
            F.lit("100_to_499_km"),
        )
        .when(
            F.col("seller_customer_distance_km") < 1000,
            F.lit("500_to_999_km"),
        )
        .when(
            F.col("seller_customer_distance_km") < 2000,
            F.lit("1000_to_1999_km"),
        )
        .otherwise(F.lit("2000_km_or_more")),
    )
    .withColumn(
        "delivery_performance_band",
        F.when(
            F.col("is_delivery_late").isNull(),
            F.lit("not_evaluated"),
        )
        .when(F.col("is_delivery_late"), F.lit("late"))
        .when(
            F.col("delivery_early_days") >= 3,
            F.lit("early_3_days_or_more"),
        )
        .otherwise(F.lit("on_time")),
    )
    .withColumn(
        "review_band",
        F.when(
            F.col("review_score_avg").isNull(),
            F.lit("not_reviewed"),
        )
        .when(F.col("review_score_avg") <= 2, F.lit("low_1_2"))
        .when(F.col("review_score_avg") == 3, F.lit("neutral_3"))
        .otherwise(F.lit("high_4_5")),
    )
)

write_silver(
    with_silver_metadata(order_service_enriched),
    "silver_order_service",
)

# COMMAND ----------

# DBTITLE 1,Validação da silver
tabelas_silver_esperadas = {
    "silver_customers",
    "silver_sellers",
    "silver_category_translation",
    "silver_products",
    "silver_orders",
    "silver_quarantine_orders",
    "silver_order_items",
    "silver_quarantine_order_items",
    "silver_payments_order_agg",
    "silver_reviews_order_agg",
    "silver_geolocation_zip_agg",
    "silver_order_service_base",
    "silver_order_service",
}

tabelas_silver_encontradas = {
    tabela.name
    for tabela in spark.catalog.listTables(f"{CATALOG}.{SILVER_SCHEMA}")
    if not tabela.isTemporary
}

faltantes = tabelas_silver_esperadas - tabelas_silver_encontradas

print(f"Tabelas Silver esperadas: {len(tabelas_silver_esperadas)}")
print(f"Tabelas Silver encontradas: {len(tabelas_silver_encontradas)}")
print(f"Tabelas Silver faltantes: {sorted(faltantes)}")

assert not faltantes, (
    "Transformação Silver incompleta. Tabelas ausentes: "
    + ", ".join(sorted(faltantes))
)

print("STATUS: APROVADO — todas as tabelas Silver foram criadas.")