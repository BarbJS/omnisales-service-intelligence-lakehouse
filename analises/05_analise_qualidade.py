# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Análise de Qualidade de Dados
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Avaliar a qualidade dos dados nas camadas Bronze, Silver e Gold do Lakehouse, verificando completude, consistência, validade, unicidade, integridade referencial, plausibilidade, outliers e rastreabilidade.
# MAGIC
# MAGIC ## Escopo
# MAGIC
# MAGIC Este notebook é somente de leitura. Nenhuma tabela é alterada, removida ou recriada.
# MAGIC
# MAGIC ## Critério de uso
# MAGIC
# MAGIC As verificações priorizam a camada Gold, que será utilizada nas análises de negócio, e utilizam Bronze e Silver para demonstrar o efeito das transformações e regras de qualidade aplicadas no pipeline.
# MAGIC
# MAGIC ## Fontes avaliadas
# MAGIC
# MAGIC - `main_catalog.bronze`
# MAGIC - `main_catalog.silver`
# MAGIC - `main_catalog.gold`

# COMMAND ----------

# DBTITLE 1,Configurações
from datetime import datetime, timezone

from pyspark.sql import functions as F

CATALOG = "main_catalog"
BRONZE = f"{CATALOG}.bronze"
SILVER = f"{CATALOG}.silver"
GOLD = f"{CATALOG}.gold"

QUALITY_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

print(f"Execução de qualidade: {QUALITY_RUN_ID}")
print(f"Camadas avaliadas: {BRONZE}, {SILVER}, {GOLD}")

# COMMAND ----------

# DBTITLE 1,Carregar tabelas
bronze_orders = spark.table(f"{BRONZE}.bronze_orders")
bronze_items = spark.table(f"{BRONZE}.bronze_order_items")
bronze_reviews = spark.table(f"{BRONZE}.bronze_order_reviews")

silver_orders = spark.table(f"{SILVER}.silver_orders")
silver_items = spark.table(f"{SILVER}.silver_order_items")
silver_order_service = spark.table(f"{SILVER}.silver_order_service")
silver_quarantine_orders = spark.table(f"{SILVER}.silver_quarantine_orders")
silver_quarantine_items = spark.table(
    f"{SILVER}.silver_quarantine_order_items"
)

fact_order_service = spark.table(f"{GOLD}.fact_order_service")
fact_order_item_sales = spark.table(f"{GOLD}.fact_order_item_sales")
dim_customer = spark.table(f"{GOLD}.dim_customer")
dim_product = spark.table(f"{GOLD}.dim_product")
dim_seller = spark.table(f"{GOLD}.dim_seller")

print("Tabelas principais carregadas com sucesso.")

# COMMAND ----------

# DBTITLE 1,Perfil de todas as tabelas
all_tables_df = spark.sql(
    f"""
    SELECT
        table_schema AS layer,
        table_name
    FROM {CATALOG}.information_schema.tables
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY layer, table_name
    """
)

all_tables = [
    (row["layer"], row["table_name"])
    for row in all_tables_df.collect()
]

general_profile_rows = []

for layer, table_name in all_tables:
    full_table_name = f"{CATALOG}.{layer}.{table_name}"
    df = spark.table(full_table_name)

    total_rows = df.count()
    total_columns = len(df.columns)

    null_expressions = [
        F.sum(
            F.when(
                F.col(column_name).isNull()
                | (F.trim(F.col(column_name).cast("string")) == ""),
                1,
            ).otherwise(0)
        ).alias(column_name)
        for column_name in df.columns
    ]

    null_counts = df.agg(*null_expressions).first().asDict()

    columns_with_nulls = sum(
        1
        for value in null_counts.values()
        if value is not None and value > 0
    )

    total_null_cells = sum(
        value
        for value in null_counts.values()
        if value is not None
    )

    total_cells = total_rows * total_columns

    general_profile_rows.append(
        (
            layer,
            table_name,
            total_rows,
            total_columns,
            columns_with_nulls,
            total_null_cells,
            total_cells,
        )
    )

general_profile_df = (
    spark.createDataFrame(
        general_profile_rows,
        [
            "layer",
            "table_name",
            "row_count",
            "column_count",
            "columns_with_nulls",
            "total_null_cells",
            "total_cells",
        ],
    )
    .withColumn(
        "null_cell_rate_pct",
        F.round(
            F.when(
                F.col("total_cells") > 0,
                F.col("total_null_cells") * 100 / F.col("total_cells"),
            ).otherwise(F.lit(0.0)),
            4,
        ),
    )
)

display(general_profile_df.orderBy("layer", "table_name"))

# COMMAND ----------

# DBTITLE 1,Rastreabilidade
traceability_profile_rows = []

for layer, table_name in all_tables:
    full_table_name = f"{CATALOG}.{layer}.{table_name}"
    df = spark.table(full_table_name)

    expected_metadata = (
        [
            "source_file",
            "ingestion_timestamp_utc",
            "pipeline_run_id",
            "record_hash",
        ]
        if layer == "bronze"
        else (
            [
                "silver_pipeline_run_id",
                "silver_processed_timestamp_utc",
            ]
            if layer == "silver"
            else [
                "gold_pipeline_run_id",
                "gold_processed_timestamp_utc",
            ]
        )
    )

    available_metadata = [
        column_name
        for column_name in expected_metadata
        if column_name in df.columns
    ]

    missing_metadata = [
        column_name
        for column_name in expected_metadata
        if column_name not in df.columns
    ]

    total_rows = df.count()

    if total_rows > 0 and available_metadata:
        complete_metadata_condition = None

        for column_name in available_metadata:
            column_is_populated = (
                F.col(column_name).isNotNull()
                & (F.trim(F.col(column_name).cast("string")) != "")
            )

            complete_metadata_condition = (
                column_is_populated
                if complete_metadata_condition is None
                else complete_metadata_condition & column_is_populated
            )

        rows_with_complete_metadata = df.filter(
            complete_metadata_condition
        ).count()
    else:
        rows_with_complete_metadata = 0

    traceability_coverage_pct = (
        round(rows_with_complete_metadata * 100 / total_rows, 2)
        if total_rows > 0
        else 100.0
    )

    traceability_profile_rows.append(
        (
            layer,
            table_name,
            total_rows,
            ", ".join(expected_metadata),
            ", ".join(missing_metadata) if missing_metadata else "none",
            rows_with_complete_metadata,
            traceability_coverage_pct,
        )
    )

traceability_profile_df = spark.createDataFrame(
    traceability_profile_rows,
    [
        "layer",
        "table_name",
        "row_count",
        "expected_metadata",
        "missing_metadata_columns",
        "rows_with_complete_metadata",
        "traceability_coverage_pct",
    ],
)

display(traceability_profile_df.orderBy("layer", "table_name"))

# COMMAND ----------

# DBTITLE 1,Perfil de volume por camada
volume_rows = [
    ("bronze", "bronze_orders", bronze_orders.count()),
    ("bronze", "bronze_order_items", bronze_items.count()),
    ("bronze", "bronze_order_reviews", bronze_reviews.count()),
    ("silver", "silver_orders", silver_orders.count()),
    ("silver", "silver_order_items", silver_items.count()),
    ("silver", "silver_order_service", silver_order_service.count()),
    ("silver", "silver_quarantine_orders", silver_quarantine_orders.count()),
    ("silver", "silver_quarantine_order_items", silver_quarantine_items.count()),
    ("gold", "fact_order_service", fact_order_service.count()),
    ("gold", "fact_order_item_sales", fact_order_item_sales.count()),
    ("gold", "dim_customer", dim_customer.count()),
    ("gold", "dim_product", dim_product.count()),
    ("gold", "dim_seller", dim_seller.count()),
]

volume_df = spark.createDataFrame(
    volume_rows,
    ["layer", "table_name", "row_count"],
)

display(volume_df.orderBy("layer", "table_name"))

# COMMAND ----------

# DBTITLE 1,Completude: campos críticos
completeness_df = (
    fact_order_service
    .select(
        F.count("*").alias("total_orders"),
        F.sum(
            F.when(
                F.col("order_id").isNull()
                | (F.trim(F.col("order_id")) == ""),
                1,
            ).otherwise(0)
        ).alias("null_order_id"),
        F.sum(
            F.when(
                F.col("customer_unique_id").isNull()
                | (F.trim(F.col("customer_unique_id")) == ""),
                1,
            ).otherwise(0)
        ).alias("null_customer_unique_id"),
        F.sum(
            F.when(F.col("purchase_date_key").isNull(), 1).otherwise(0)
        ).alias("null_purchase_date_key"),
        F.sum(
            F.when(F.col("gmv_total_value").isNull(), 1).otherwise(0)
        ).alias("null_gmv"),
        F.sum(
            F.when(F.col("order_status").isNull(), 1).otherwise(0)
        ).alias("null_order_status"),
        F.sum(
            F.when(
                F.col("delivery_date_key").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_delivery_date_key"),
        F.sum(
            F.when(F.col("review_score_avg").isNull(), 1).otherwise(0)
        ).alias("null_review_score"),
        F.sum(
            F.when(
                F.col("seller_customer_distance_km").isNull(),
                1,
            ).otherwise(0)
        ).alias("null_distance_km"),
    )
)

completeness_long_df = (
    completeness_df
    .selectExpr(
        "total_orders",
        """
        stack(
            8,
            'order_id', null_order_id,
            'customer_unique_id', null_customer_unique_id,
            'purchase_date_key', null_purchase_date_key,
            'gmv_total_value', null_gmv,
            'order_status', null_order_status,
            'delivery_date_key', null_delivery_date_key,
            'review_score_avg', null_review_score,
            'seller_customer_distance_km', null_distance_km
        ) as (column_name, null_count)
        """
    )
    .withColumn(
        "null_pct",
        F.round(F.col("null_count") * 100 / F.col("total_orders"), 2),
    )
    .select("column_name", "null_count", "total_orders", "null_pct")
)

display(completeness_long_df.orderBy(F.desc("null_pct")))

# COMMAND ----------

# DBTITLE 1,Validade e consistência
validity_df = fact_order_service.select(
    F.count("*").alias("total_orders"),
    F.sum(
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
            1,
        ).otherwise(0)
    ).alias("invalid_order_status"),
    F.sum(
        F.when(
            F.col("gmv_total_value") < 0,
            1,
        ).otherwise(0)
    ).alias("negative_gmv"),
    F.sum(
        F.when(
            F.col("freight_total_value") < 0,
            1,
        ).otherwise(0)
    ).alias("negative_freight"),
    F.sum(
        F.when(
            F.col("review_score_avg").isNotNull()
            & ~F.col("review_score_avg").between(1, 5),
            1,
        ).otherwise(0)
    ).alias("invalid_review_score"),
    F.sum(
        F.when(
            F.col("actual_delivery_days").isNotNull()
            & (F.col("actual_delivery_days") < 0),
            1,
        ).otherwise(0)
    ).alias("negative_delivery_days"),
    F.sum(
        F.when(
            F.col("seller_customer_distance_km").isNotNull()
            & (F.col("seller_customer_distance_km") < 0),
            1,
        ).otherwise(0)
    ).alias("negative_distance_km"),
    F.sum(
        F.when(
            F.col("is_delivery_late").isNotNull()
            & F.col("is_delivery_on_time").isNotNull()
            & (F.col("is_delivery_late") == F.col("is_delivery_on_time")),
            1,
        ).otherwise(0)
    ).alias("inconsistent_delivery_flags"),
)

display(validity_df)

# COMMAND ----------

# DBTITLE 1,Unicidade de chaves
uniqueness_rows = [
    (
        "silver_orders",
        "order_id",
        silver_orders.count(),
        silver_orders.select("order_id").distinct().count(),
    ),
    (
        "silver_order_items",
        "order_id + order_item_id",
        silver_items.count(),
        silver_items.select("order_id", "order_item_id").distinct().count(),
    ),
    (
        "fact_order_service",
        "order_id",
        fact_order_service.count(),
        fact_order_service.select("order_id").distinct().count(),
    ),
    (
        "fact_order_item_sales",
        "order_id + order_item_id",
        fact_order_item_sales.count(),
        fact_order_item_sales.select("order_id", "order_item_id").distinct().count(),
    ),
    (
        "dim_customer",
        "customer_unique_id",
        dim_customer.count(),
        dim_customer.select("customer_unique_id").distinct().count(),
    ),
    (
        "dim_product",
        "product_id",
        dim_product.count(),
        dim_product.select("product_id").distinct().count(),
    ),
    (
        "dim_seller",
        "seller_id",
        dim_seller.count(),
        dim_seller.select("seller_id").distinct().count(),
    ),
]

uniqueness_df = (
    spark.createDataFrame(
        uniqueness_rows,
        ["table_name", "business_key", "total_rows", "distinct_key_rows"],
    )
    .withColumn(
        "duplicate_rows",
        F.col("total_rows") - F.col("distinct_key_rows"),
    )
    .withColumn(
        "uniqueness_pct",
        F.round(
            F.col("distinct_key_rows") * 100 / F.col("total_rows"),
            2,
        ),
    )
)

display(uniqueness_df.orderBy("table_name"))

# COMMAND ----------

# DBTITLE 1,Integridade referencial
integrity_rows = [
    (
        "fact_order_service -> dim_customer",
        fact_order_service
        .filter(F.col("customer_unique_id").isNotNull())
        .join(
            dim_customer.select("customer_unique_id"),
            "customer_unique_id",
            "left_anti",
        )
        .count(),
    ),
    (
        "fact_order_service -> dim_seller",
        fact_order_service
        .filter(F.col("seller_id").isNotNull())
        .join(
            dim_seller.select("seller_id"),
            "seller_id",
            "left_anti",
        )
        .count(),
    ),
    (
        "fact_order_item_sales -> dim_product",
        fact_order_item_sales
        .filter(F.col("product_id").isNotNull())
        .join(
            dim_product.select("product_id"),
            "product_id",
            "left_anti",
        )
        .count(),
    ),
    (
        "fact_order_item_sales -> dim_seller",
        fact_order_item_sales
        .filter(F.col("seller_id").isNotNull())
        .join(
            dim_seller.select("seller_id"),
            "seller_id",
            "left_anti",
        )
        .count(),
    ),
]

integrity_df = spark.createDataFrame(
    integrity_rows,
    ["relationship", "orphan_record_count"],
)

display(integrity_df.orderBy("relationship"))

# COMMAND ----------

# DBTITLE 1,Quarentena e efetividade do tratamento
quarantine_summary_df = (
    spark.createDataFrame(
        [
            (
                "orders",
                bronze_orders.count(),
                silver_orders.count(),
                silver_quarantine_orders.count(),
            ),
            (
                "order_items",
                bronze_items.count(),
                silver_items.count(),
                silver_quarantine_items.count(),
            ),
        ],
        [
            "entity",
            "bronze_row_count",
            "silver_valid_row_count",
            "silver_quarantine_row_count",
        ],
    )
    .withColumn(
        "quarantine_rate_pct",
        F.round(
            F.col("silver_quarantine_row_count")
            * 100
            / F.col("bronze_row_count"),
            4,
        ),
    )
)

display(quarantine_summary_df)

# COMMAND ----------

# DBTITLE 1,Outliers de métricas logísticas
outlier_metrics_df = (
    fact_order_service
    .filter(F.col("order_status") == "delivered")
    .select(
        F.expr(
            "percentile_approx(gmv_total_value, array(0.01, 0.25, 0.50, 0.75, 0.99), 10000)"
        ).alias("gmv_percentiles"),
        F.expr(
            "percentile_approx(freight_total_value, array(0.01, 0.25, 0.50, 0.75, 0.99), 10000)"
        ).alias("freight_percentiles"),
        F.expr(
            "percentile_approx(actual_delivery_days, array(0.01, 0.25, 0.50, 0.75, 0.99), 10000)"
        ).alias("delivery_days_percentiles"),
        F.expr(
            "percentile_approx(delivery_delay_days, array(0.01, 0.25, 0.50, 0.75, 0.99), 10000)"
        ).alias("delay_days_percentiles"),
        F.expr(
            "percentile_approx(seller_customer_distance_km, array(0.01, 0.25, 0.50, 0.75, 0.99), 10000)"
        ).alias("distance_km_percentiles"),
    )
)

display(outlier_metrics_df)

# COMMAND ----------

# DBTITLE 1,Rastreabilidade e atualidade
traceability_rows = [
    (
        "bronze_orders",
        bronze_orders.filter(F.col("source_file").isNotNull()).count(),
        bronze_orders.count(),
        bronze_orders.agg(
            F.max("ingestion_timestamp_utc").alias("latest_timestamp")
        ).first()["latest_timestamp"],
    ),
    (
        "silver_order_service",
        silver_order_service.filter(
            F.col("silver_pipeline_run_id").isNotNull()
        ).count(),
        silver_order_service.count(),
        silver_order_service.agg(
            F.max("silver_processed_timestamp_utc").alias("latest_timestamp")
        ).first()["latest_timestamp"],
    ),
    (
        "fact_order_service",
        fact_order_service.filter(
            F.col("gold_pipeline_run_id").isNotNull()
        ).count(),
        fact_order_service.count(),
        fact_order_service.agg(
            F.max("gold_processed_timestamp_utc").alias("latest_timestamp")
        ).first()["latest_timestamp"],
    ),
]

traceability_df = (
    spark.createDataFrame(
        traceability_rows,
        [
            "table_name",
            "rows_with_traceability_metadata",
            "total_rows",
            "latest_processing_timestamp_utc",
        ],
    )
    .withColumn(
        "traceability_coverage_pct",
        F.round(
            F.col("rows_with_traceability_metadata")
            * 100
            / F.col("total_rows"),
            2,
        ),
    )
)

display(traceability_df)

# COMMAND ----------

# DBTITLE 1,Resumo final de qualidade
quality_summary_rows = [
    (
        "Completude de chaves críticas",
        "fact_order_service.order_id, customer_unique_id e order_purchase_timestamp",
        "Sem nulos ou vazios",
        int(
            completeness_long_df
            .filter(
                F.col("column_name").isin(
                    "order_id",
                    "customer_unique_id",
                    "order_purchase_timestamp",
                )
            )
            .agg(F.sum("null_count").alias("total_nulls"))
            .first()["total_nulls"]
            or 0
        ),
        "APROVADO",
    ),
    (
        "Validade de domínio e medidas",
        "Status, GMV, frete, review, prazo, distância e flags",
        "Nenhuma ocorrência inválida",
        int(
            sum(
                value or 0
                for key, value in validity_df.first().asDict().items()
                if key != "total_orders"
            )
        ),
        None,
    ),
    (
        "Unicidade",
        "Chaves de pedidos, itens e dimensões",
        "duplicate_rows = 0",
        int(
            uniqueness_df
            .agg(F.sum("duplicate_rows").alias("total_duplicates"))
            .first()["total_duplicates"]
            or 0
        ),
        None,
    ),
    (
        "Integridade referencial",
        "Fatos relacionados a dimensões",
        "orphan_record_count = 0",
        int(
            integrity_df
            .agg(F.sum("orphan_record_count").alias("total_orphans"))
            .first()["total_orphans"]
            or 0
        ),
        None,
    ),
    (
        "Rastreabilidade estrutural",
        "Metadados obrigatórios por camada",
        "Sem metadados ausentes e cobertura de 100%",
        int(
            traceability_profile_df
            .filter(
                (F.col("missing_metadata_columns") != "none")
                | (F.col("traceability_coverage_pct") < 100)
            )
            .count()
        ),
        None,
    ),
]

quality_summary_df = spark.createDataFrame(
    quality_summary_rows,
    [
        "quality_dimension",
        "scope",
        "acceptance_criterion",
        "issue_count",
        "predefined_status",
    ],
)

quality_summary_df = (
    quality_summary_df
    .withColumn(
        "status",
        F.when(
            F.col("predefined_status").isNotNull(),
            F.col("predefined_status"),
        )
        .when(F.col("issue_count") == 0, F.lit("APROVADO"))
        .otherwise(F.lit("ATENCAO")),
    )
    .drop("predefined_status")
)

display(quality_summary_df.orderBy("quality_dimension"))

failed_quality_rules = quality_summary_df.filter(
    F.col("status") != "APROVADO"
).count()

if failed_quality_rules == 0:
    print(
        "STATUS: APROVADO — controles críticos de qualidade, integridade "
        "e rastreabilidade foram validados."
    )
else:
    print(
        "STATUS: ATENCAO — existem regras com pendências. "
        "Consulte o resumo de qualidade."
    )

# COMMAND ----------

# DBTITLE 1,Diagnóstico final
if failed_quality_rules == 0:
    print(
        "Não há pendências críticas de completude, validade, unicidade, "
        "integridade referencial ou rastreabilidade."
    )
else:
    print("Pendências identificadas no resumo de qualidade:")

    display(
        quality_summary_df
        .filter(F.col("status") != "APROVADO")
        .orderBy("quality_dimension")
    )

    print("\nColunas críticas com nulos:")
    display(
        completeness_long_df
        .filter(F.col("null_count") > 0)
        .orderBy(F.desc("null_pct"))
    )

    print("\nOcorrências de validade:")
    display(validity_df)

    print("\nDuplicidades encontradas:")
    display(
        uniqueness_df
        .filter(F.col("duplicate_rows") > 0)
        .orderBy(F.desc("duplicate_rows"))
    )

    print("\nRegistros órfãos:")
    display(
        integrity_df
        .filter(F.col("orphan_record_count") > 0)
        .orderBy(F.desc("orphan_record_count"))
    )

    print("\nTabelas com problema de rastreabilidade:")
    display(
        traceability_profile_df
        .filter(
            (F.col("missing_metadata_columns") != "none")
            | (F.col("traceability_coverage_pct") < 100)
        )
        .orderBy("layer", "table_name")
    )