# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Modelagem Dimensional da Camada Gold
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Criar o modelo dimensional e os marts analíticos da camada Gold a partir das tabelas tratadas da camada Silver.
# MAGIC
# MAGIC ## Princípios aplicados
# MAGIC
# MAGIC - Consumo exclusivo de tabelas Silver;
# MAGIC - Modelagem dimensional em esquema estrela;
# MAGIC - Separação entre fato no grão de pedido e fato no grão de item;
# MAGIC - Dimensões reutilizáveis para tempo, cliente, produto, seller, localidade e faixas de serviço;
# MAGIC - Marts agregados para análise de confiabilidade logística, satisfação do cliente e desempenho comercial;
# MAGIC - Persistência de todas as tabelas no schema `main_catalog.gold`.
# MAGIC
# MAGIC ## Tabelas de saída
# MAGIC
# MAGIC - Dimensões: `dim_date`, `dim_customer`, `dim_product`, `dim_seller`, `dim_location`, `dim_service_band`;
# MAGIC - Fatos: `fact_order_service`, `fact_order_item_sales`;
# MAGIC - Marts: `mart_logistics_performance`, `mart_service_risk`, `mart_customer_experience`, `mart_monthly_operations`.

# COMMAND ----------

# DBTITLE 1,Configuração e funções
from datetime import datetime, timezone

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F

CATALOG = "main_catalog"
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

SILVER = f"{CATALOG}.{SILVER_SCHEMA}"
GOLD = f"{CATALOG}.{GOLD_SCHEMA}"

PIPELINE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

def write_gold(df: DataFrame, table_name: str) -> None:
    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"{GOLD}.{table_name}")
    )
    print(f"Tabela Gold criada: {GOLD}.{table_name} | Linhas: {df.count()}")

def with_gold_metadata(df: DataFrame) -> DataFrame:
    return (
        df
        .withColumn("gold_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
        .withColumn("gold_processed_timestamp_utc", F.current_timestamp())
    )

print(f"Origem Silver: {SILVER}")
print(f"Destino Gold: {GOLD}")
print(f"Pipeline run ID: {PIPELINE_RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Carregar fontes silver
orders_service = spark.table(f"{SILVER}.silver_order_service")
order_items = spark.table(f"{SILVER}.silver_order_items")
customers = spark.table(f"{SILVER}.silver_customers")
products = spark.table(f"{SILVER}.silver_products")
categories = spark.table(f"{SILVER}.silver_category_translation")
sellers = spark.table(f"{SILVER}.silver_sellers")

print("Tabelas Silver carregadas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Criar dim_date
date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date",
]

all_dates = None

for date_column in date_columns:
    current_dates = (
        orders_service
        .select(F.to_date(F.col(date_column)).alias("full_date"))
        .filter(F.col("full_date").isNotNull())
    )

    all_dates = current_dates if all_dates is None else all_dates.unionByName(current_dates)

dim_date = (
    all_dates
    .distinct()
    .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
    .withColumn("year", F.year("full_date"))
    .withColumn("month_number", F.month("full_date"))
    .withColumn("month_name", F.date_format("full_date", "MMMM"))
    .withColumn("year_month", F.date_format("full_date", "yyyy-MM"))
    .withColumn("quarter", F.quarter("full_date"))
    .withColumn("day_of_month", F.dayofmonth("full_date"))
    .withColumn("day_of_week_number", F.dayofweek("full_date"))
    .withColumn("day_of_week_name", F.date_format("full_date", "EEEE"))
    .withColumn(
        "is_weekend",
        F.dayofweek("full_date").isin(1, 7),
    )
    .orderBy("full_date")
)

write_gold(with_gold_metadata(dim_date), "dim_date")

# COMMAND ----------

# DBTITLE 1,Criar dim_costumer
customer_metrics = (
    orders_service
    .filter(F.col("customer_unique_id").isNotNull())
    .groupBy("customer_unique_id")
    .agg(
        F.countDistinct("order_id").alias("lifetime_order_count"),
        F.round(F.sum("gmv_total_value"), 2).alias("lifetime_gmv"),
        F.round(F.avg("gmv_total_value"), 2).alias("lifetime_avg_ticket"),
        F.min("order_purchase_timestamp").alias("first_order_timestamp"),
        F.max("order_purchase_timestamp").alias("last_order_timestamp"),
    )
)

customer_reference = (
    orders_service
    .filter(F.col("customer_unique_id").isNotNull())
    .select(
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "customer_zip_code_prefix",
    )
    .dropDuplicates(["customer_unique_id"])
)

dim_customer = (
    customer_metrics
    .join(customer_reference, "customer_unique_id", "left")
    .withColumn(
        "is_repeat_customer",
        F.col("lifetime_order_count") > 1,
    )
    .withColumn(
        "customer_value_band",
        F.when(F.col("lifetime_gmv") < 100, F.lit("under_100"))
        .when(F.col("lifetime_gmv") < 300, F.lit("100_to_299"))
        .when(F.col("lifetime_gmv") < 600, F.lit("300_to_599"))
        .otherwise(F.lit("600_or_more")),
    )
)

write_gold(with_gold_metadata(dim_customer), "dim_customer")

# COMMAND ----------

# DBTITLE 1,Criar dim_product
dim_product = (
    products.alias("p")
    .join(
        categories.alias("c"),
        F.col("p.product_category_name") == F.col("c.product_category_name"),
        "left",
    )
    .select(
        F.col("p.product_id"),
        F.col("p.product_category_name"),
        F.coalesce(
            F.col("c.product_category_name_english"),
            F.lit("not_available"),
        ).alias("product_category_name_english"),
        F.col("p.product_name_length"),
        F.col("p.product_description_length"),
        F.col("p.product_photos_qty"),
        F.col("p.product_weight_g"),
        F.col("p.product_length_cm"),
        F.col("p.product_height_cm"),
        F.col("p.product_width_cm"),
    )
)

write_gold(with_gold_metadata(dim_product), "dim_product")

# COMMAND ----------

# DBTITLE 1,Criar dim_seller
seller_metrics = (
    order_items
    .groupBy("seller_id")
    .agg(
        F.countDistinct("order_id").alias("lifetime_order_count"),
        F.round(F.sum("price"), 2).alias("lifetime_product_revenue"),
        F.round(F.sum("freight_value"), 2).alias("lifetime_freight_value"),
    )
)

dim_seller = (
    sellers.alias("s")
    .join(
        seller_metrics.alias("m"),
        F.col("s.seller_id") == F.col("m.seller_id"),
        "left",
    )
    .select(
        F.col("s.seller_id"),
        F.col("s.seller_zip_code_prefix"),
        F.col("s.seller_city"),
        F.col("s.seller_state"),
        F.coalesce(F.col("m.lifetime_order_count"), F.lit(0)).alias(
            "lifetime_order_count"
        ),
        F.coalesce(F.col("m.lifetime_product_revenue"), F.lit(0.0)).alias(
            "lifetime_product_revenue"
        ),
        F.coalesce(F.col("m.lifetime_freight_value"), F.lit(0.0)).alias(
            "lifetime_freight_value"
        ),
    )
)

write_gold(with_gold_metadata(dim_seller), "dim_seller")

# COMMAND ----------

# DBTITLE 1,Criar dim_location
customer_locations = (
    orders_service
    .select(
        F.lit("customer").alias("location_role"),
        F.col("customer_zip_code_prefix").alias("zip_code_prefix"),
        F.col("customer_city").alias("city"),
        F.col("customer_state").alias("state"),
        F.col("customer_latitude_geo").alias("latitude"),
        F.col("customer_longitude_geo").alias("longitude"),
    )
)

seller_locations = (
    orders_service
    .select(
        F.lit("seller").alias("location_role"),
        F.col("seller_zip_code_prefix").alias("zip_code_prefix"),
        F.col("seller_city").alias("city"),
        F.col("seller_state").alias("state"),
        F.col("seller_latitude_geo").alias("latitude"),
        F.col("seller_longitude_geo").alias("longitude"),
    )
)

dim_location = (
    customer_locations
    .unionByName(seller_locations)
    .filter(F.col("zip_code_prefix").isNotNull())
    .dropDuplicates(["location_role", "zip_code_prefix", "city", "state"])
)

write_gold(with_gold_metadata(dim_location), "dim_location")

# COMMAND ----------

# DBTITLE 1,Criar dim_service_band
dim_service_band = (
    orders_service
    .select(
        "distance_band",
        "delivery_performance_band",
        "review_band",
    )
    .fillna("not_available")
    .dropDuplicates()
    .withColumn(
        "service_band_key",
        F.sha2(
            F.concat_ws(
                "||",
                F.col("distance_band"),
                F.col("delivery_performance_band"),
                F.col("review_band"),
            ),
            256,
        ),
    )
    .select(
        "service_band_key",
        "distance_band",
        "delivery_performance_band",
        "review_band",
    )
)

write_gold(with_gold_metadata(dim_service_band), "dim_service_band")

# COMMAND ----------

# DBTITLE 1,Criar fact_order_service
service_band = spark.table(f"{GOLD}.dim_service_band")

fact_order_service = (
    orders_service.alias("o")
    .join(
        service_band.alias("b"),
        (
            F.coalesce(F.col("o.distance_band"), F.lit("not_available"))
            == F.col("b.distance_band")
        )
        & (
            F.coalesce(
                F.col("o.delivery_performance_band"),
                F.lit("not_available"),
            )
            == F.col("b.delivery_performance_band")
        )
        & (
            F.coalesce(F.col("o.review_band"), F.lit("not_available"))
            == F.col("b.review_band")
        ),
        "left",
    )
    .select(
        F.col("o.order_id"),
        F.date_format(
            F.to_date(F.col("o.order_purchase_timestamp")),
            "yyyyMMdd",
        ).cast("int").alias("purchase_date_key"),
        F.date_format(
            F.to_date(F.col("o.order_delivered_customer_date")),
            "yyyyMMdd",
        ).cast("int").alias("delivery_date_key"),
        F.col("o.customer_unique_id"),
        F.col("o.customer_zip_code_prefix"),
        F.col("o.primary_seller_id").alias("seller_id"),
        F.col("b.service_band_key"),
        F.col("o.order_status"),
        F.col("o.order_item_count"),
        F.col("o.distinct_product_count"),
        F.col("o.distinct_seller_count"),
        F.col("o.product_total_value"),
        F.col("o.freight_total_value"),
        F.col("o.gmv_total_value"),
        F.col("o.payment_total_value"),
        F.col("o.payment_record_count"),
        F.col("o.max_payment_installments"),
        F.col("o.payment_types"),
        F.col("o.review_score_avg"),
        F.col("o.review_count"),
        F.col("o.actual_delivery_days"),
        F.col("o.delivery_delay_days"),
        F.col("o.delivery_early_days"),
        F.col("o.is_delivered"),
        F.col("o.is_delivery_late"),
        F.col("o.is_delivery_on_time"),
        F.col("o.seller_count"),
        F.col("o.seller_customer_distance_km"),
        F.col("o.distance_band"),
        F.col("o.delivery_performance_band"),
        F.col("o.review_band"),
        F.col("o.sales_channel"),
    )
)

write_gold(with_gold_metadata(fact_order_service), "fact_order_service")

# COMMAND ----------

# DBTITLE 1,Criar fact_order_item_sales
fact_order_item_sales = (
    order_items.alias("i")
    .join(
        orders_service.alias("o"),
        F.col("i.order_id") == F.col("o.order_id"),
        "inner",
    )
    .select(
        F.col("i.order_id"),
        F.col("i.order_item_id"),
        F.date_format(
            F.to_date(F.col("o.order_purchase_timestamp")),
            "yyyyMMdd",
        ).cast("int").alias("purchase_date_key"),
        F.col("o.customer_unique_id"),
        F.col("o.customer_zip_code_prefix"),
        F.col("i.product_id"),
        F.col("i.seller_id"),
        F.col("i.shipping_limit_date"),
        F.col("i.price").alias("product_price"),
        F.col("i.freight_value"),
        F.round(
            F.col("i.price") + F.col("i.freight_value"),
            2,
        ).alias("item_gmv"),
        F.round(
            F.when(
                F.col("i.price") > 0,
                F.col("i.freight_value") / F.col("i.price"),
            ),
            4,
        ).alias("freight_to_price_ratio"),
        F.col("o.order_status"),
        F.col("o.is_delivery_late"),
        F.col("o.is_delivery_on_time"),
        F.col("o.review_score_avg"),
        F.col("o.seller_customer_distance_km"),
        F.col("o.distance_band"),
        F.col("o.sales_channel"),
    )
)

write_gold(with_gold_metadata(fact_order_item_sales), "fact_order_item_sales")

# COMMAND ----------

# DBTITLE 1,Criar mart_logistics_performance
mart_logistics_performance = (
    fact_order_service
    .filter(
        (F.col("order_status") == "delivered")
        & F.col("is_delivery_late").isNotNull()
    )
    .groupBy("seller_id")
    .agg(
        F.countDistinct("order_id").alias("delivered_order_count"),
        F.round(F.sum("gmv_total_value"), 2).alias("gmv_total_value"),
        F.round(F.avg("actual_delivery_days"), 2).alias(
            "avg_delivery_days"
        ),
        F.round(
            F.avg(F.when(F.col("is_delivery_late"), F.lit(1.0)).otherwise(F.lit(0.0)))
            * 100,
            2,
        ).alias("late_delivery_rate_pct"),
        F.round(F.avg("delivery_delay_days"), 2).alias(
            "avg_delay_days"
        ),
        F.round(F.avg("freight_total_value"), 2).alias(
            "avg_freight_value"
        ),
        F.round(F.avg("review_score_avg"), 2).alias(
            "avg_review_score"
        ),
        F.round(
            F.avg(
                F.when(F.col("review_score_avg") <= 2, F.lit(1.0))
                .otherwise(F.lit(0.0))
            )
            * 100,
            2,
        ).alias("low_review_rate_pct"),
    )
)

write_gold(
    with_gold_metadata(mart_logistics_performance),
    "mart_logistics_performance",
)

# COMMAND ----------

# DBTITLE 1,Criar mart_service_risk
mart_service_risk = (
    mart_logistics_performance
    .withColumn(
        "service_risk_level",
        F.when(
            (F.col("late_delivery_rate_pct") >= 20)
            | (F.col("avg_review_score") < 3.5),
            F.lit("high"),
        )
        .when(
            (F.col("late_delivery_rate_pct") >= 10)
            | (F.col("avg_review_score") < 4.0),
            F.lit("medium"),
        )
        .otherwise(F.lit("low")),
    )
    .withColumn(
        "priority_rank",
        F.row_number().over(
            Window.orderBy(
                F.desc("service_risk_level"),
                F.desc("gmv_total_value"),
                F.desc("late_delivery_rate_pct"),
            )
        ),
    )
)

write_gold(with_gold_metadata(mart_service_risk), "mart_service_risk")

# COMMAND ----------

# DBTITLE 1,Criar mart_customer_experience
mart_customer_experience = (
    fact_order_service
    .filter(F.col("order_status") == "delivered")
    .groupBy(
        "distance_band",
        "is_delivery_late",
    )
    .agg(
        F.countDistinct("order_id").alias("delivered_order_count"),
        F.round(F.avg("actual_delivery_days"), 2).alias(
            "avg_delivery_days"
        ),
        F.round(F.avg("freight_total_value"), 2).alias(
            "avg_freight_value"
        ),
        F.round(F.avg("review_score_avg"), 2).alias(
            "avg_review_score"
        ),
        F.round(
            F.avg(
                F.when(F.col("review_score_avg") <= 2, F.lit(1.0))
                .otherwise(F.lit(0.0))
            )
            * 100,
            2,
        ).alias("low_review_rate_pct"),
    )
)

write_gold(
    with_gold_metadata(mart_customer_experience),
    "mart_customer_experience",
)

# COMMAND ----------

# DBTITLE 1,Criar mart_monthly_operations
mart_monthly_operations = (
    fact_order_service
    .filter(F.col("purchase_date_key").isNotNull())
    .withColumn(
        "purchase_year_month",
        F.date_format(
            F.to_date(F.col("purchase_date_key").cast("string"), "yyyyMMdd"),
            "yyyy-MM",
        ),
    )
    .groupBy("purchase_year_month")
    .agg(
        F.countDistinct("order_id").alias("order_count"),
        F.round(F.sum("gmv_total_value"), 2).alias("gmv_total_value"),
        F.round(F.avg("gmv_total_value"), 2).alias("avg_ticket_value"),
        F.round(F.avg("actual_delivery_days"), 2).alias(
            "avg_delivery_days"
        ),
        F.round(
            F.avg(
                F.when(F.col("is_delivery_late"), F.lit(1.0))
                .otherwise(F.lit(0.0))
            )
            * 100,
            2,
        ).alias("late_delivery_rate_pct"),
        F.round(F.avg("review_score_avg"), 2).alias(
            "avg_review_score"
        ),
    )
    .orderBy("purchase_year_month")
)

write_gold(
    with_gold_metadata(mart_monthly_operations),
    "mart_monthly_operations",
)

# COMMAND ----------

# DBTITLE 1,Validação gold
tabelas_gold_esperadas = {
    "dim_date",
    "dim_customer",
    "dim_product",
    "dim_seller",
    "dim_location",
    "dim_service_band",
    "fact_order_service",
    "fact_order_item_sales",
    "mart_logistics_performance",
    "mart_service_risk",
    "mart_customer_experience",
    "mart_monthly_operations",
}

tabelas_gold_encontradas = {
    tabela.name
    for tabela in spark.catalog.listTables(f"{CATALOG}.{GOLD_SCHEMA}")
    if not tabela.isTemporary
}

faltantes = tabelas_gold_esperadas - tabelas_gold_encontradas

print(f"Tabelas Gold esperadas: {len(tabelas_gold_esperadas)}")
print(f"Tabelas Gold encontradas: {len(tabelas_gold_encontradas)}")
print(f"Tabelas Gold faltantes: {sorted(faltantes)}")

assert not faltantes, (
    "Modelagem Gold incompleta. Tabelas ausentes: "
    + ", ".join(sorted(faltantes))
)

print("STATUS: APROVADO — todas as tabelas Gold foram criadas.")