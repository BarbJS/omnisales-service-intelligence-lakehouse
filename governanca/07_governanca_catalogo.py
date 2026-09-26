# Databricks notebook source
# MAGIC %md
# MAGIC # 07 — Governança, Catálogo e Segurança de Dados
# MAGIC
# MAGIC ## Objetivo
# MAGIC
# MAGIC Documentar, classificar e auditar os ativos de dados das camadas Bronze, Silver e Gold do MVP OmniSales Service Intelligence Lakehouse no Unity Catalog.
# MAGIC
# MAGIC ## Escopo
# MAGIC
# MAGIC - Comentários de contexto para todas as tabelas;
# MAGIC - Comentários para todas as colunas;
# MAGIC - Tags de camada, domínio, granularidade, classificação, qualidade, origem e latência-alvo;
# MAGIC - Inventário automatizado de metadados;
# MAGIC - Auditoria de ownership e permissões do catálogo e schemas;
# MAGIC - Registro de princípios de segurança aplicáveis ao ambiente acadêmico.
# MAGIC
# MAGIC ## Convenções de latência
# MAGIC
# MAGIC Como a fonte Olist é histórica e carregada manualmente, as latências são metas arquiteturais, não métricas observadas de produção:
# MAGIC
# MAGIC - Bronze: D+0 após o recebimento do arquivo;
# MAGIC - Silver: D+0 após a conclusão da Bronze;
# MAGIC - Gold: D+0 após a conclusão da Silver;
# MAGIC - Disponibilidade de negócio de ponta a ponta: até D+1.
# MAGIC
# MAGIC ## Limites do ambiente
# MAGIC
# MAGIC O workspace é individual e acadêmico. Não serão concedidos privilégios a identidades fictícias. O controle de acesso será auditado e documentado por meio do Unity Catalog, mantendo-se o princípio do menor privilégio como política de evolução futura.

# COMMAND ----------

# DBTITLE 1,Configurações
from datetime import datetime, timezone
from pyspark.sql import functions as F

CATALOG = "main_catalog"
SCHEMAS = ["bronze", "silver", "gold"]

GOVERNANCE_RUN_ID = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

LAYER_LATENCY = {
    "bronze": "D+0_after_file_receipt",
    "silver": "D+0_after_bronze",
    "gold": "D+0_after_silver",
}

COMMON_TAGS = {
    "project": "omnisales_service_intelligence",
    "data_source": "olist_brazilian_ecommerce",
    "usage_scope": "academic_non_commercial",
    "refresh_pattern": "batch_manual_mvp",
}

print(f"Catálogo: {CATALOG}")
print(f"Schemas: {SCHEMAS}")
print(f"Execução de governança: {GOVERNANCE_RUN_ID}")

# COMMAND ----------

# DBTITLE 1,Funções de apoio
def escape_sql_text(value: str) -> str:
    return str(value).replace("'", "''")

def quote_identifier(identifier: str) -> str:
    return f"`{identifier}`"

def comment_table(schema_name: str, table_name: str, description: str) -> None:
    spark.sql(
        f"""
        COMMENT ON TABLE {CATALOG}.{schema_name}.{quote_identifier(table_name)}
        IS '{escape_sql_text(description)}'
        """
    )

def comment_column(
    schema_name: str,
    table_name: str,
    column_name: str,
    description: str,
) -> None:
    spark.sql(
        f"""
        ALTER TABLE {CATALOG}.{schema_name}.{quote_identifier(table_name)}
        ALTER COLUMN {quote_identifier(column_name)}
        COMMENT '{escape_sql_text(description)}'
        """
    )

def set_table_tags(schema_name: str, table_name: str, tags: dict) -> None:
    tags_sql = ", ".join(
        f"'{escape_sql_text(key)}' = '{escape_sql_text(value)}'"
        for key, value in tags.items()
    )

    spark.sql(
        f"""
        ALTER TABLE {CATALOG}.{schema_name}.{quote_identifier(table_name)}
        SET TAGS ({tags_sql})
        """
    )

def set_column_tags(
    schema_name: str,
    table_name: str,
    column_name: str,
    tags: dict,
) -> None:
    tags_sql = ", ".join(
        f"'{escape_sql_text(key)}' = '{escape_sql_text(value)}'"
        for key, value in tags.items()
    )

    spark.sql(
        f"""
        ALTER TABLE {CATALOG}.{schema_name}.{quote_identifier(table_name)}
        ALTER COLUMN {quote_identifier(column_name)}
        SET TAGS ({tags_sql})
        """
    )

def build_table_tags(
    layer: str,
    domain: str,
    grain: str,
    classification: str,
    quality_status: str,
) -> dict:
    return {
        **COMMON_TAGS,
        "data_layer": layer,
        "business_domain": domain,
        "data_grain": grain,
        "data_classification": classification,
        "quality_status": quality_status,
        "latency_target": LAYER_LATENCY[layer],
    }

# COMMAND ----------

# DBTITLE 1,Documentação de tabelas bronze
bronze_table_docs = {
    "bronze_orders": {
        "description": (
            "Objetivo: preservar os pedidos recebidos do arquivo "
            "olist_orders_dataset.csv sem transformação de negócio. "
            "Granularidade: uma linha por order_id conforme a fonte. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem rastreável para tratamento de pedidos na Silver."
        ),
        "tags": build_table_tags(
            "bronze", "sales_logistics", "order",
            "anonymized", "raw"
        ),
    },
    "bronze_order_items": {
        "description": (
            "Objetivo: preservar itens de pedidos do arquivo "
            "olist_order_items_dataset.csv sem transformação de negócio. "
            "Granularidade: uma linha por order_id e order_item_id. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem de preço, frete, produto e seller."
        ),
        "tags": build_table_tags(
            "bronze", "sales_logistics", "order_item",
            "anonymized", "raw"
        ),
    },
    "bronze_order_payments": {
        "description": (
            "Objetivo: preservar registros de pagamento do arquivo "
            "olist_order_payments_dataset.csv sem transformação. "
            "Granularidade: uma linha por order_id e payment_sequential. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem para consolidação de pagamentos por pedido."
        ),
        "tags": build_table_tags(
            "bronze", "payments", "order_payment",
            "anonymized", "raw"
        ),
    },
    "bronze_order_reviews": {
        "description": (
            "Objetivo: preservar avaliações de pedidos do arquivo "
            "olist_order_reviews_dataset.csv sem transformação. "
            "Granularidade: uma linha por review_id. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem para análise de satisfação do cliente."
        ),
        "tags": build_table_tags(
            "bronze", "customer_experience", "review",
            "anonymized", "raw"
        ),
    },
    "bronze_customers": {
        "description": (
            "Objetivo: preservar registros anonimizados de clientes do arquivo "
            "olist_customers_dataset.csv sem transformação. "
            "Granularidade: uma linha por customer_id associado a pedido. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem para identificação anonimizada e localização aproximada."
        ),
        "tags": build_table_tags(
            "bronze", "customer", "customer_order_record",
            "anonymized", "raw"
        ),
    },
    "bronze_products": {
        "description": (
            "Objetivo: preservar registros de produtos do arquivo "
            "olist_products_dataset.csv sem transformação. "
            "Granularidade: uma linha por product_id. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem de categoria e atributos físicos de produto."
        ),
        "tags": build_table_tags(
            "bronze", "product_catalog", "product",
            "internal", "raw"
        ),
    },
    "bronze_sellers": {
        "description": (
            "Objetivo: preservar registros anonimizados de sellers do arquivo "
            "olist_sellers_dataset.csv sem transformação. "
            "Granularidade: uma linha por seller_id. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem de seller e localização aproximada de origem."
        ),
        "tags": build_table_tags(
            "bronze", "seller", "seller",
            "anonymized", "raw"
        ),
    },
    "bronze_geolocation": {
        "description": (
            "Objetivo: preservar geolocalizações aproximadas do arquivo "
            "olist_geolocation_dataset.csv sem transformação. "
            "Granularidade: uma linha por registro de coordenada associado a "
            "prefixo de CEP; múltiplas linhas podem existir para o mesmo CEP. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem para estimativa agregada de distância."
        ),
        "tags": build_table_tags(
            "bronze", "geolocation", "zip_code_coordinate_record",
            "sensitive_approximate_location", "raw"
        ),
    },
    "bronze_category_translation": {
        "description": (
            "Objetivo: preservar a tradução de categorias do arquivo "
            "product_category_name_translation.csv sem transformação. "
            "Granularidade: uma linha por categoria em português. "
            "Camada: Bronze. Latência-alvo: D+0 após recebimento do arquivo. "
            "Uso: origem para padronização de categorias na Gold."
        ),
        "tags": build_table_tags(
            "bronze", "product_catalog", "product_category",
            "internal", "raw"
        ),
    },
}

# COMMAND ----------

# DBTITLE 1,Documentação de tabelas silver
silver_table_docs = {
    "silver_customers": {
        "description": (
            "Objetivo: disponibilizar clientes tratados para relacionamentos "
            "operacionais. Granularidade: uma linha por customer_id. "
            "Transformações: identificação, CEP, cidade e UF padronizados; "
            "duplicatas removidas. Camada: Silver. Latência-alvo: D+0 após Bronze. "
            "Uso: origem confiável para dimensões e fatos Gold."
        ),
        "tags": build_table_tags(
            "silver", "customer", "customer_order_record",
            "anonymized", "validated"
        ),
    },
    "silver_sellers": {
        "description": (
            "Objetivo: disponibilizar sellers tratados para relacionamentos "
            "operacionais. Granularidade: uma linha por seller_id. "
            "Transformações: CEP, cidade e UF padronizados; duplicatas removidas. "
            "Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "seller", "seller",
            "anonymized", "validated"
        ),
    },
    "silver_category_translation": {
        "description": (
            "Objetivo: padronizar traduções de categorias de produto. "
            "Granularidade: uma linha por product_category_name. "
            "Transformações: normalização de texto e deduplicação. "
            "Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "product_catalog", "product_category",
            "internal", "validated"
        ),
    },
    "silver_products": {
        "description": (
            "Objetivo: disponibilizar produtos tratados. Granularidade: uma linha "
            "por product_id. Transformações: conversão de atributos numéricos, "
            "normalização de categoria, correção de nomes de colunas e deduplicação. "
            "Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "product_catalog", "product",
            "internal", "validated"
        ),
    },
    "silver_orders": {
        "description": (
            "Objetivo: disponibilizar pedidos válidos e tipados. Granularidade: "
            "uma linha por order_id. Transformações: status normalizado, eventos "
            "temporais convertidos e duplicatas removidas. Registros inválidos "
            "são direcionados à quarentena. Camada: Silver. Latência-alvo: "
            "D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "sales_logistics", "order",
            "anonymized", "validated"
        ),
    },
    "silver_quarantine_orders": {
        "description": (
            "Objetivo: reter pedidos que falharam em regras mínimas de qualidade. "
            "Granularidade: uma linha por registro de pedido inválido. "
            "Motivos: chave ausente, status inválido ou data de compra inválida. "
            "Camada: Silver. Latência-alvo: D+0 após Bronze. "
            "Uso: auditoria; não utilizar em análises Gold."
        ),
        "tags": build_table_tags(
            "silver", "data_quality", "invalid_order_record",
            "anonymized", "quarantine"
        ),
    },
    "silver_order_items": {
        "description": (
            "Objetivo: disponibilizar itens de pedido válidos e tipados. "
            "Granularidade: uma linha por order_id e order_item_id. "
            "Transformações: preço e frete convertidos para decimal, chaves "
            "validadas e duplicatas removidas. Camada: Silver. Latência-alvo: "
            "D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "sales_logistics", "order_item",
            "anonymized", "validated"
        ),
    },
    "silver_quarantine_order_items": {
        "description": (
            "Objetivo: reter itens de pedido inválidos. Granularidade: uma linha "
            "por registro inválido de item. Motivos: chaves obrigatórias ausentes "
            "ou preço/frete nulo ou negativo. Camada: Silver. Latência-alvo: "
            "D+0 após Bronze. Uso: auditoria; não utilizar em análises Gold."
        ),
        "tags": build_table_tags(
            "silver", "data_quality", "invalid_order_item_record",
            "anonymized", "quarantine"
        ),
    },
    "silver_payments_order_agg": {
        "description": (
            "Objetivo: consolidar pagamentos no grão de pedido para prevenir "
            "multiplicação de linhas em joins. Granularidade: uma linha por order_id. "
            "Transformações: tipagem, validação e agregação de valor total, "
            "quantidade de registros, parcelas e modalidades. Camada: Silver. "
            "Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "payments", "order",
            "anonymized", "validated"
        ),
    },
    "silver_reviews_order_agg": {
        "description": (
            "Objetivo: consolidar avaliações válidas no grão de pedido. "
            "Granularidade: uma linha por order_id. Transformações: validação "
            "de nota entre 1 e 5, conversão tolerante de timestamps, deduplicação "
            "e agregação de score. Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "customer_experience", "order",
            "anonymized", "validated"
        ),
    },
    "silver_geolocation_zip_agg": {
        "description": (
            "Objetivo: consolidar geolocalização por prefixo de CEP para enriquecer "
            "dados sem multiplicar linhas. Granularidade: uma linha por zip_code_prefix. "
            "Transformações: validação de coordenadas plausíveis no Brasil e média "
            "de latitude/longitude. Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "geolocation", "zip_code_prefix",
            "sensitive_approximate_location", "validated"
        ),
    },
    "silver_order_service_base": {
        "description": (
            "Objetivo: consolidar a visão operacional base por pedido. Granularidade: "
            "uma linha por order_id. Integra pedidos, clientes, itens, pagamentos e "
            "avaliações previamente agregados. Inclui GMV, frete, pagamento, review, "
            "prazo e atraso. Camada: Silver. Latência-alvo: D+0 após Bronze."
        ),
        "tags": build_table_tags(
            "silver", "logistics_customer_experience", "order",
            "anonymized", "validated"
        ),
    },
    "silver_order_service": {
        "description": (
            "Objetivo: disponibilizar visão operacional enriquecida por pedido para "
            "análises logísticas. Granularidade: uma linha por order_id. Amplia a "
            "tabela base com seller de referência, localização aproximada, distância "
            "seller-cliente e faixas de serviço/satisfação. Camada: Silver. "
            "Latência-alvo: D+0 após Bronze. Para análises de distância, priorizar "
            "seller_count igual a 1."
        ),
        "tags": build_table_tags(
            "silver", "logistics_customer_experience", "order",
            "anonymized", "validated"
        ),
    },
}

# COMMAND ----------

# DBTITLE 1,Documentação de tabelas gold
gold_table_docs = {
    "dim_date": {
        "description": (
            "Objetivo: fornecer contexto temporal para análises. Granularidade: "
            "uma linha por full_date. Contém atributos de ano, mês, trimestre, "
            "dia da semana e fim de semana derivados de eventos de pedidos. "
            "Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "shared_dimension", "calendar_date",
            "internal", "business_ready"
        ),
    },
    "dim_customer": {
        "description": (
            "Objetivo: fornecer dimensão de clientes anonimizados para análises de "
            "recorrência e valor. Granularidade: uma linha por customer_unique_id. "
            "Inclui métricas históricas de pedidos, GMV, ticket, primeira e última "
            "compra e faixa de valor. Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "customer", "unique_customer",
            "anonymized", "business_ready"
        ),
    },
    "dim_product": {
        "description": (
            "Objetivo: fornecer dimensão de produtos. Granularidade: uma linha por "
            "product_id. Inclui categoria, tradução e atributos físicos. "
            "Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "product_catalog", "product",
            "internal", "business_ready"
        ),
    },
    "dim_seller": {
        "description": (
            "Objetivo: fornecer dimensão de sellers anonimizados. Granularidade: "
            "uma linha por seller_id. Inclui localidade e métricas históricas de "
            "pedidos, receita de produto e frete. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "seller", "seller",
            "anonymized", "business_ready"
        ),
    },
    "dim_location": {
        "description": (
            "Objetivo: fornecer dimensão de localização aproximada de cliente e "
            "seller. Granularidade: uma linha por location_role, zip_code_prefix, "
            "city e state. Não representa endereço individual. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "geolocation", "location_role_zip_city_state",
            "sensitive_approximate_location", "business_ready"
        ),
    },
    "dim_service_band": {
        "description": (
            "Objetivo: padronizar classificações de distância, desempenho de entrega "
            "e avaliação. Granularidade: uma linha por combinação de distance_band, "
            "delivery_performance_band e review_band. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "logistics_customer_experience", "service_band_combination",
            "internal", "business_ready"
        ),
    },
    "fact_order_service": {
        "description": (
            "Objetivo: registrar métricas comerciais, logísticas, pagamento e "
            "satisfação no grão de pedido. Granularidade: uma linha por order_id. "
            "Inclui GMV, frete, prazo, atraso, seller, distância aproximada e "
            "avaliação. Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "logistics_customer_experience", "order",
            "anonymized", "business_ready"
        ),
    },
    "fact_order_item_sales": {
        "description": (
            "Objetivo: registrar medidas comerciais no grão de item de pedido. "
            "Granularidade: uma linha por order_id e order_item_id. Inclui preço, "
            "frete, GMV do item, produto e seller. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "sales_logistics", "order_item",
            "anonymized", "business_ready"
        ),
    },
    "mart_logistics_performance": {
        "description": (
            "Objetivo: disponibilizar KPIs logísticos agregados por seller. "
            "Granularidade: uma linha por seller_id. Inclui pedidos entregues, GMV, "
            "prazo, taxa de atraso, frete e satisfação. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "logistics_performance", "seller",
            "anonymized", "business_ready"
        ),
    },
    "mart_service_risk": {
        "description": (
            "Objetivo: priorizar risco operacional de sellers. Granularidade: uma "
            "linha por seller_id. Classifica risco como low, medium ou high segundo "
            "taxa de atraso e nota média, com ordenação por prioridade e GMV. "
            "Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "logistics_risk", "seller",
            "anonymized", "business_ready"
        ),
    },
    "mart_customer_experience": {
        "description": (
            "Objetivo: analisar experiência do cliente por faixa de distância e "
            "condição de atraso. Granularidade: uma linha por distance_band e "
            "is_delivery_late. Inclui pedidos entregues, prazo, frete e avaliação. "
            "Camada: Gold. Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "customer_experience", "distance_band_delivery_late",
            "anonymized", "business_ready"
        ),
    },
    "mart_monthly_operations": {
        "description": (
            "Objetivo: acompanhar indicadores mensais de operação e vendas. "
            "Granularidade: uma linha por purchase_year_month. Inclui pedidos, GMV, "
            "ticket médio, prazo, atraso e satisfação. Camada: Gold. "
            "Latência-alvo: D+0 após Silver."
        ),
        "tags": build_table_tags(
            "gold", "operations_analytics", "purchase_year_month",
            "anonymized", "business_ready"
        ),
    },
}

# COMMAND ----------

# DBTITLE 1,Aplicar descrições e tags em tabelas
from pyspark.sql.types import StructField, StructType, StringType

all_table_docs = {
    "bronze": bronze_table_docs,
    "silver": silver_table_docs,
    "gold": gold_table_docs,
}

table_documentation_results = []

for schema_name, table_docs in all_table_docs.items():
    for table_name, metadata in table_docs.items():
        try:
            comment_table(
                schema_name,
                table_name,
                metadata["description"],
            )

            set_table_tags(
                schema_name,
                table_name,
                metadata["tags"],
            )

            table_documentation_results.append(
                (
                    schema_name,
                    table_name,
                    "APPLIED",
                    None,
                )
            )
        except Exception as error:
            table_documentation_results.append(
                (
                    schema_name,
                    table_name,
                    "ERROR",
                    str(error),
                )
            )

table_documentation_schema = StructType(
    [
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

table_documentation_df = spark.createDataFrame(
    table_documentation_results,
    schema=table_documentation_schema,
)

display(
    table_documentation_df
    .orderBy("schema_name", "table_name")
)

errors = (
    table_documentation_df
    .filter(F.col("status") == "ERROR")
    .count()
)

assert errors == 0, (
    "Falha ao aplicar comentários ou tags em uma ou mais tabelas. "
    "Consulte a coluna error_message."
)

print(
    "STATUS: APROVADO — comentários e tags aplicados a todas as "
    "tabelas Bronze, Silver e Gold."
)

# COMMAND ----------

# DBTITLE 1,Inventário real de tabelas e colunas
inventory_df = spark.sql(
    f"""
    SELECT
        table_schema AS schema_name,
        table_name,
        column_name,
        data_type,
        is_nullable AS nullable,
        comment AS existing_comment,
        ordinal_position
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY
        table_schema,
        table_name,
        ordinal_position
    """
)

display(inventory_df)

# COMMAND ----------

# DBTITLE 1,Comments reutilizáveis
common_column_comments = {
    "source_file": (
        "Metadado técnico de ingestão. Nome do arquivo CSV de origem "
        "que forneceu o registro."
    ),
    "ingestion_timestamp_utc": (
        "Metadado técnico de ingestão. Data e hora em UTC em que o "
        "registro foi persistido na camada Bronze."
    ),
    "pipeline_run_id": (
        "Metadado técnico de ingestão. Identificador lógico da execução "
        "do notebook que carregou o registro na camada Bronze."
    ),
    "record_hash": (
        "Metadado técnico de rastreabilidade. Hash SHA-256 calculado a "
        "partir do conteúdo original da linha, utilizado para auditoria "
        "e apoio à identificação de duplicidade."
    ),
    "silver_pipeline_run_id": (
        "Metadado técnico de processamento. Identificador lógico da "
        "execução que gerou ou atualizou o registro na camada Silver."
    ),
    "silver_processed_timestamp_utc": (
        "Metadado técnico de processamento. Data e hora em UTC em que "
        "o registro foi processado na camada Silver."
    ),
    "gold_pipeline_run_id": (
        "Metadado técnico de processamento. Identificador lógico da "
        "execução que gerou ou atualizou o registro na camada Gold."
    ),
    "gold_processed_timestamp_utc": (
        "Metadado técnico de processamento. Data e hora em UTC em que "
        "o registro foi processado na camada Gold."
    ),
}

status_domain_comment = (
    "Domínio permitido: created, approved, invoiced, processing, shipped, "
    "delivered, unavailable ou canceled."
)

boolean_domain_comment = "Domínio permitido: true, false ou null quando não avaliável."

review_score_domain_comment = "Domínio permitido: números inteiros de 1 a 5; null quando não informado ou inválido."

# COMMAND ----------

# DBTITLE 1,Comments das colunas das tabelas bronze
bronze_column_docs = {
    "bronze_category_translation": {
        "product_category_name": (
            "Nome original da categoria de produto, em português, conforme "
            "recebido da fonte Olist. Chave de relacionamento com produtos."
        ),
        "product_category_name_english": (
            "Tradução em inglês da categoria de produto fornecida pela fonte Olist."
        ),
    },
    "bronze_customers": {
        "customer_id": (
            "Identificador técnico anonimizado do cliente associado ao pedido. "
            "Usado para relacionar clientes e pedidos."
        ),
        "customer_unique_id": (
            "Identificador persistente anonimizado do cliente. Usado para "
            "identificar recorrência entre pedidos sem expor identidade pessoal."
        ),
        "customer_zip_code_prefix": (
            "Prefixo de cinco dígitos do CEP do cliente, conforme recebido da "
            "fonte. Representa localização aproximada, não endereço completo."
        ),
        "customer_city": "Cidade informada para o cliente, conforme recebida da fonte.",
        "customer_state": (
            "Unidade Federativa informada para o cliente. Domínio esperado: "
            "siglas de UFs brasileiras."
        ),
    },
    "bronze_geolocation": {
        "geolocation_zip_code_prefix": (
            "Prefixo de cinco dígitos do CEP associado ao registro de geolocalização."
        ),
        "geolocation_lat": (
            "Latitude aproximada associada ao prefixo de CEP, preservada como texto "
            "na Bronze; convertida para número na Silver."
        ),
        "geolocation_lng": (
            "Longitude aproximada associada ao prefixo de CEP, preservada como texto "
            "na Bronze; convertida para número na Silver."
        ),
        "geolocation_city": (
            "Cidade associada ao prefixo de CEP, conforme recebida da fonte."
        ),
        "geolocation_state": (
            "Unidade Federativa associada ao prefixo de CEP. Domínio esperado: "
            "siglas de UFs brasileiras."
        ),
    },
    "bronze_order_items": {
        "order_id": "Identificador técnico do pedido ao qual o item pertence.",
        "order_item_id": (
            "Número sequencial do item dentro do pedido, preservado como texto na Bronze."
        ),
        "product_id": "Identificador técnico do produto adquirido no item.",
        "seller_id": "Identificador técnico anonimizado do seller responsável pelo item.",
        "shipping_limit_date": (
            "Data e hora limite de envio do item, preservada como texto na Bronze."
        ),
        "price": (
            "Preço do produto no item, preservado como texto na Bronze; convertido "
            "para decimal na Silver."
        ),
        "freight_value": (
            "Valor do frete associado ao item, preservado como texto na Bronze; "
            "convertido para decimal na Silver."
        ),
    },
    "bronze_order_payments": {
        "order_id": "Identificador técnico do pedido associado ao pagamento.",
        "payment_sequential": (
            "Número sequencial do registro de pagamento dentro do pedido, preservado "
            "como texto na Bronze."
        ),
        "payment_type": (
            "Modalidade de pagamento informada pela fonte. Valores conhecidos na base "
            "Olist incluem boleto, credit_card, debit_card, voucher e not_defined."
        ),
        "payment_installments": (
            "Quantidade de parcelas informada para o pagamento, preservada como texto "
            "na Bronze."
        ),
        "payment_value": (
            "Valor financeiro do registro de pagamento, preservado como texto na Bronze; "
            "convertido para decimal na Silver."
        ),
    },
    "bronze_order_reviews": {
        "review_id": "Identificador técnico da avaliação do cliente.",
        "order_id": "Identificador técnico do pedido associado à avaliação.",
        "review_score": (
            "Nota de satisfação atribuída pelo cliente, preservada como texto na Bronze. "
            "Domínio esperado na fonte: inteiros de 1 a 5."
        ),
        "review_comment_title": "Título textual opcional da avaliação do cliente.",
        "review_comment_message": "Mensagem textual opcional da avaliação do cliente.",
        "review_creation_date": (
            "Data e hora de criação da avaliação, preservada como texto na Bronze."
        ),
        "review_answer_timestamp": (
            "Data e hora de resposta à avaliação, preservada como texto na Bronze."
        ),
    },
    "bronze_orders": {
        "order_id": "Identificador técnico único do pedido.",
        "customer_id": "Identificador técnico anonimizado do cliente associado ao pedido.",
        "order_status": f"Status operacional do pedido conforme a fonte. {status_domain_comment}",
        "order_purchase_timestamp": (
            "Data e hora de criação da compra, preservada como texto na Bronze."
        ),
        "order_approved_at": (
            "Data e hora de aprovação do pedido/pagamento, preservada como texto na Bronze."
        ),
        "order_delivered_carrier_date": (
            "Data e hora de disponibilização do pedido à transportadora, preservada "
            "como texto na Bronze."
        ),
        "order_delivered_customer_date": (
            "Data e hora de entrega ao cliente, preservada como texto na Bronze."
        ),
        "order_estimated_delivery_date": (
            "Data e hora estimada de entrega ao cliente, preservada como texto na Bronze."
        ),
    },
    "bronze_products": {
        "product_id": "Identificador técnico do produto.",
        "product_category_name": (
            "Categoria original do produto em português, conforme recebida da fonte."
        ),
        "product_name_lenght": (
            "Quantidade de caracteres no nome do produto. O nome da coluna possui "
            "erro ortográfico na fonte e foi preservado na Bronze."
        ),
        "product_description_lenght": (
            "Quantidade de caracteres na descrição do produto. O nome da coluna possui "
            "erro ortográfico na fonte e foi preservado na Bronze."
        ),
        "product_photos_qty": (
            "Quantidade de fotos cadastradas para o produto, preservada como texto na Bronze."
        ),
        "product_weight_g": (
            "Peso do produto em gramas, preservado como texto na Bronze."
        ),
        "product_length_cm": (
            "Comprimento do produto em centímetros, preservado como texto na Bronze."
        ),
        "product_height_cm": (
            "Altura do produto em centímetros, preservada como texto na Bronze."
        ),
        "product_width_cm": (
            "Largura do produto em centímetros, preservada como texto na Bronze."
        ),
    },
    "bronze_sellers": {
        "seller_id": "Identificador técnico anonimizado do seller.",
        "seller_zip_code_prefix": (
            "Prefixo de cinco dígitos do CEP do seller; representa localização aproximada."
        ),
        "seller_city": "Cidade do seller, conforme recebida da fonte.",
        "seller_state": (
            "Unidade Federativa do seller. Domínio esperado: siglas de UFs brasileiras."
        ),
    },
}

# COMMAND ----------

# DBTITLE 1,Comments das colunas das tabelas silver
silver_column_docs = {
    "silver_category_translation": {
        "product_category_name": (
            "Nome normalizado da categoria original em português. Chave de relacionamento "
            "com produtos. Texto em minúsculas e sem espaços excedentes."
        ),
        "product_category_name_english": (
            "Tradução normalizada da categoria para inglês, fornecida pela fonte."
        ),
    },
    "silver_customers": {
        "customer_id": "Identificador técnico anonimizado do cliente associado ao pedido.",
        "customer_unique_id": (
            "Identificador persistente anonimizado de cliente, utilizado para medir "
            "recorrência entre pedidos."
        ),
        "customer_zip_code_prefix": (
            "Prefixo de CEP do cliente padronizado como texto de cinco dígitos; "
            "representa localização aproximada."
        ),
        "customer_city": "Cidade do cliente padronizada para capitalização de palavras.",
        "customer_state": (
            "UF do cliente padronizada em letras maiúsculas. Domínio esperado: siglas "
            "de UFs brasileiras."
        ),
    },
    "silver_geolocation_zip_agg": {
        "zip_code_prefix": (
            "Prefixo de CEP padronizado como texto de cinco dígitos; chave de agregação "
            "de geolocalização."
        ),
        "latitude": (
            "Latitude média aproximada do prefixo de CEP, após validação de limites "
            "geográficos plausíveis para o Brasil."
        ),
        "longitude": (
            "Longitude média aproximada do prefixo de CEP, após validação de limites "
            "geográficos plausíveis para o Brasil."
        ),
        "geolocation_city": "Cidade associada ao prefixo de CEP após padronização.",
        "geolocation_state": (
            "UF associada ao prefixo de CEP em letras maiúsculas. Domínio esperado: "
            "siglas de UFs brasileiras."
        ),
        "geolocation_record_count": (
            "Quantidade de registros de geolocalização de origem agregados para o prefixo de CEP."
        ),
    },
    "silver_order_items": {
        "order_id": "Identificador técnico do pedido ao qual o item pertence.",
        "order_item_id": "Número sequencial do item dentro do pedido. Domínio: inteiro positivo.",
        "product_id": "Identificador técnico do produto adquirido.",
        "seller_id": "Identificador técnico anonimizado do seller responsável pelo item.",
        "shipping_limit_date": "Data e hora limite de envio do item.",
        "price": "Preço do produto no item. Domínio esperado: decimal maior ou igual a zero.",
        "freight_value": "Valor do frete do item. Domínio esperado: decimal maior ou igual a zero.",
    },
    "silver_orders": {
        "order_id": "Identificador técnico único do pedido.",
        "customer_id": "Identificador técnico anonimizado do cliente associado ao pedido.",
        "order_status": f"Status operacional do pedido normalizado. {status_domain_comment}",
        "order_purchase_timestamp": "Data e hora de criação da compra.",
        "order_approved_at": "Data e hora de aprovação do pedido/pagamento.",
        "order_delivered_carrier_date": (
            "Data e hora de disponibilização do pedido à transportadora."
        ),
        "order_delivered_customer_date": "Data e hora de entrega ao cliente.",
        "order_estimated_delivery_date": "Data e hora estimada de entrega ao cliente.",
    },
    "silver_payments_order_agg": {
        "order_id": "Identificador técnico do pedido; chave da agregação de pagamentos.",
        "payment_total_value": (
            "Soma dos valores de pagamento válidos do pedido. Domínio esperado: decimal maior ou igual a zero."
        ),
        "payment_record_count": "Quantidade de registros de pagamento válidos do pedido.",
        "max_payment_installments": (
            "Maior quantidade de parcelas entre os pagamentos válidos do pedido. "
            "Domínio esperado: inteiro maior ou igual a zero."
        ),
        "payment_types": (
            "Modalidades de pagamento distintas do pedido, concatenadas por '|'. "
            "Valores possíveis por modalidade: boleto, credit_card, debit_card, voucher e not_defined."
        ),
    },
    "silver_products": {
        "product_id": "Identificador técnico do produto.",
        "product_category_name": "Categoria do produto normalizada em minúsculas.",
        "product_name_length": (
            "Quantidade de caracteres no nome do produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_description_length": (
            "Quantidade de caracteres na descrição do produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_photos_qty": (
            "Quantidade de fotos cadastradas para o produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_weight_g": "Peso do produto em gramas. Domínio esperado: número maior ou igual a zero.",
        "product_length_cm": "Comprimento do produto em centímetros. Domínio esperado: número maior ou igual a zero.",
        "product_height_cm": "Altura do produto em centímetros. Domínio esperado: número maior ou igual a zero.",
        "product_width_cm": "Largura do produto em centímetros. Domínio esperado: número maior ou igual a zero.",
    },
    "silver_quarantine_order_items": {
        "order_id": "Identificador do pedido do registro em quarentena; pode ser nulo ou inválido.",
        "order_item_id": "Sequencial do item do registro em quarentena; pode ser nulo ou inválido.",
        "product_id": "Identificador do produto do registro em quarentena; pode ser nulo ou inválido.",
        "seller_id": "Identificador do seller do registro em quarentena; pode ser nulo ou inválido.",
        "shipping_limit_date": "Data limite de envio do registro em quarentena, quando interpretável.",
        "price": "Preço do registro em quarentena; pode ser nulo ou negativo.",
        "freight_value": "Frete do registro em quarentena; pode ser nulo ou negativo.",
        "quarantine_reason": (
            "Motivo ou combinação de motivos que levou o registro à quarentena. "
            "Valores possíveis incluem: order_id ausente, order_item_id inválido, "
            "product_id ausente, seller_id ausente, preço inválido e frete inválido."
        ),
    },
    "silver_quarantine_orders": {
        "order_id": "Identificador do pedido em quarentena; pode ser nulo ou inválido.",
        "customer_id": "Identificador do cliente em quarentena; pode ser nulo ou inválido.",
        "order_status": "Status do pedido em quarentena; pode estar fora do domínio permitido.",
        "order_purchase_timestamp": "Data de compra do registro em quarentena; pode ser nula ou inválida.",
        "order_approved_at": "Data de aprovação do pedido em quarentena, quando interpretável.",
        "order_delivered_carrier_date": "Data de entrega à transportadora do registro em quarentena, quando interpretável.",
        "order_delivered_customer_date": "Data de entrega ao cliente do registro em quarentena, quando interpretável.",
        "order_estimated_delivery_date": "Data estimada de entrega do registro em quarentena, quando interpretável.",
        "quarantine_reason": (
            "Motivo ou combinação de motivos de quarentena. Valores possíveis incluem: "
            "order_id ausente, customer_id ausente, data de compra inválida ou ausente "
            "e status de pedido inválido."
        ),
    },
    "silver_reviews_order_agg": {
        "order_id": "Identificador técnico do pedido; chave da agregação de avaliações.",
        "review_score_avg": "Média das notas válidas do pedido. Domínio: número entre 1 e 5.",
        "review_score_min": "Menor nota válida registrada para o pedido. Domínio: inteiro de 1 a 5.",
        "review_score_max": "Maior nota válida registrada para o pedido. Domínio: inteiro de 1 a 5.",
        "review_count": "Quantidade de avaliações válidas associadas ao pedido.",
        "latest_review_creation_date": "Data e hora mais recente de criação de avaliação válida do pedido.",
    },
    "silver_sellers": {
        "seller_id": "Identificador técnico anonimizado do seller.",
        "seller_zip_code_prefix": "Prefixo de CEP do seller padronizado como texto de cinco dígitos.",
        "seller_city": "Cidade do seller padronizada para capitalização de palavras.",
        "seller_state": "UF do seller em letras maiúsculas. Domínio esperado: siglas de UFs brasileiras.",
    },
}

# COMMAND ----------

# DBTITLE 1,Complemento comments das tabelas silver
silver_order_service_common_docs = {
    "order_id": (
        "Identificador técnico único do pedido. Chave de negócio da tabela no "
        "grão de pedido."
    ),
    "customer_id": (
        "Identificador técnico anonimizado do cliente associado ao pedido."
    ),
    "customer_unique_id": (
        "Identificador persistente anonimizado do cliente, utilizado para "
        "identificar recorrência entre pedidos."
    ),
    "customer_zip_code_prefix": (
        "Prefixo de CEP do cliente padronizado como texto de cinco dígitos; "
        "representa localização aproximada, não endereço completo."
    ),
    "customer_city": (
        "Cidade do cliente após padronização de texto e capitalização."
    ),
    "customer_state": (
        "UF do cliente padronizada em letras maiúsculas. Domínio esperado: "
        "siglas de UFs brasileiras."
    ),
    "order_status": (
        f"Status operacional do pedido normalizado. {status_domain_comment}"
    ),
    "order_purchase_timestamp": (
        "Data e hora de criação da compra; marco inicial do prazo de entrega."
    ),
    "order_approved_at": (
        "Data e hora de aprovação do pedido ou pagamento, quando disponível."
    ),
    "order_delivered_carrier_date": (
        "Data e hora de disponibilização do pedido à transportadora, quando disponível."
    ),
    "order_delivered_customer_date": (
        "Data e hora em que o pedido foi entregue ao cliente, quando disponível."
    ),
    "order_estimated_delivery_date": (
        "Data e hora estimada de entrega ao cliente; referência para avaliar SLA."
    ),
    "product_total_value": (
        "Soma dos preços dos itens do pedido, sem incluir frete. Domínio esperado: "
        "decimal maior ou igual a zero."
    ),
    "freight_total_value": (
        "Soma dos valores de frete dos itens do pedido. Domínio esperado: decimal "
        "maior ou igual a zero."
    ),
    "gmv_total_value": (
        "Valor bruto do pedido, calculado como preço dos itens mais frete. "
        "Domínio esperado: decimal maior ou igual a zero."
    ),
    "order_item_count": (
        "Quantidade de itens válidos associados ao pedido. Domínio esperado: "
        "inteiro positivo."
    ),
    "distinct_product_count": (
        "Quantidade de produtos distintos no pedido. Domínio esperado: inteiro positivo."
    ),
    "distinct_seller_count": (
        "Quantidade de sellers distintos no pedido. Domínio esperado: inteiro positivo."
    ),
    "avg_freight_to_price_ratio": (
        "Média da razão freight_value/price entre itens com preço positivo. "
        "Representa o peso relativo do frete; domínio esperado: número maior ou igual a zero."
    ),
    "payment_total_value": (
        "Soma dos valores de pagamento válidos associados ao pedido. Domínio esperado: "
        "decimal maior ou igual a zero."
    ),
    "payment_record_count": (
        "Quantidade de registros de pagamento válidos associados ao pedido. "
        "Um pedido pode ter mais de um registro."
    ),
    "max_payment_installments": (
        "Maior número de parcelas entre os pagamentos válidos do pedido. "
        "Domínio esperado: inteiro maior ou igual a zero."
    ),
    "payment_types": (
        "Modalidades de pagamento distintas do pedido, concatenadas por '|'. "
        "Valores possíveis por modalidade: boleto, credit_card, debit_card, "
        "voucher e not_defined."
    ),
    "review_score_avg": (
        "Média das notas válidas de avaliação do pedido. Domínio esperado: "
        "número entre 1 e 5; null quando não há avaliação válida."
    ),
    "review_score_min": (
        "Menor nota válida de avaliação do pedido. Domínio esperado: inteiro de 1 a 5; "
        "null quando não há avaliação válida."
    ),
    "review_score_max": (
        "Maior nota válida de avaliação do pedido. Domínio esperado: inteiro de 1 a 5; "
        "null quando não há avaliação válida."
    ),
    "review_count": (
        "Quantidade de avaliações válidas associadas ao pedido. Domínio esperado: inteiro maior ou igual a zero."
    ),
    "latest_review_creation_date": (
        "Data e hora mais recente de criação de avaliação válida associada ao pedido."
    ),
    "actual_delivery_days": (
        "Prazo real de entrega em dias, calculado entre a compra e a entrega ao cliente. "
        "Null quando a entrega não está disponível."
    ),
    "delivery_delay_days": (
        "Dias de atraso do pedido. Calculado como a diferença positiva entre entrega real "
        "e entrega estimada; zero para pedidos entregues no prazo; null quando não avaliável."
    ),
    "delivery_early_days": (
        "Dias de antecedência da entrega. Calculado como a diferença positiva entre entrega "
        "estimada e entrega real; zero para pedidos atrasados; null quando não avaliável."
    ),
    "is_delivered": (
        "Indicador de pedido entregue. Domínio permitido: true quando order_status é delivered; "
        "false nos demais status."
    ),
    "is_delivery_late": (
        "Indicador de atraso logístico. Domínio permitido: true quando a entrega real ocorreu "
        "após a data estimada; false quando ocorreu no prazo; null quando as datas não permitem avaliação."
    ),
    "is_delivery_on_time": (
        "Indicador de entrega no prazo. Domínio permitido: true quando a entrega real ocorreu "
        "até a data estimada; false quando ocorreu após a estimativa; null quando não avaliável."
    ),
    "is_low_review": (
        "Indicador de satisfação baixa. Domínio permitido: true quando review_score_avg é menor "
        "ou igual a 2; false quando a nota é maior que 2; null quando não há avaliação."
    ),
    "sales_channel": (
        "Canal de venda do pedido. Domínio atual do MVP: marketplace_web. O modelo foi preparado "
        "para extensões futuras, como loja_fisica, web_proprio e app."
    ),
}

silver_column_docs.update(
    {
        "silver_order_service_base": {
            **silver_order_service_common_docs,
        },
        "silver_order_service": {
            **silver_order_service_common_docs,
            "primary_seller_id": (
                "Seller de referência do pedido utilizado para estimativa de distância. "
                "Para análises de distância, priorizar pedidos com seller_count igual a 1."
            ),
            "seller_count": (
                "Quantidade de sellers distintos associados ao pedido. Domínio esperado: "
                "inteiro positivo."
            ),
            "seller_zip_code_prefix": (
                "Prefixo de CEP do seller de referência, padronizado como texto de cinco dígitos. "
                "Representa localização aproximada."
            ),
            "seller_city": (
                "Cidade padronizada do seller de referência."
            ),
            "seller_state": (
                "UF padronizada do seller de referência. Domínio esperado: siglas de UFs brasileiras."
            ),
            "seller_latitude_geo": (
                "Latitude aproximada do seller de referência, obtida de geolocalização agregada por CEP."
            ),
            "seller_longitude_geo": (
                "Longitude aproximada do seller de referência, obtida de geolocalização agregada por CEP."
            ),
            "customer_latitude_geo": (
                "Latitude aproximada do cliente, obtida de geolocalização agregada por CEP."
            ),
            "customer_longitude_geo": (
                "Longitude aproximada do cliente, obtida de geolocalização agregada por CEP."
            ),
            "seller_customer_distance_km": (
                "Distância geográfica aproximada, em quilômetros, entre seller de referência e cliente, "
                "calculada pela fórmula de Haversine com coordenadas agregadas por CEP. Não representa "
                "a rota real de transporte."
            ),
            "distance_band": (
                "Faixa da distância aproximada seller-cliente. Domínio permitido: under_100_km, "
                "100_to_499_km, 500_to_999_km, 1000_to_1999_km, 2000_km_or_more ou not_available."
            ),
            "delivery_performance_band": (
                "Classificação de desempenho de entrega. Domínio permitido: late, on_time, "
                "early_3_days_or_more ou not_evaluated."
            ),
            "review_band": (
                "Classificação da satisfação do pedido. Domínio permitido: low_1_2, neutral_3, "
                "high_4_5 ou not_reviewed."
            ),
        },
    }
)

print(
    "Comentários das tabelas centrais Silver adicionados: "
    "silver_order_service_base e silver_order_service."
)

# COMMAND ----------

# DBTITLE 1,Comments das colunas das tabelas gold
gold_column_docs = {
    "dim_date": {
        "full_date": "Data de calendário no formato DATE. Chave natural da dimensão temporal.",
        "date_key": (
            "Chave substituta de data no formato inteiro AAAAMMDD. Exemplo: 20180131."
        ),
        "year": "Ano da data de calendário. Domínio esperado: inteiro de quatro dígitos.",
        "month_number": "Número do mês. Domínio permitido: inteiros de 1 a 12.",
        "month_name": "Nome do mês derivado da data de calendário.",
        "year_month": "Período mensal no formato AAAA-MM.",
        "quarter": "Trimestre do ano. Domínio permitido: inteiros de 1 a 4.",
        "day_of_month": "Dia do mês. Domínio permitido: inteiros de 1 a 31.",
        "day_of_week_number": (
            "Número do dia da semana conforme função dayofweek do Spark. "
            "Domínio permitido: 1 para domingo até 7 para sábado."
        ),
        "day_of_week_name": "Nome do dia da semana derivado da data de calendário.",
        "is_weekend": (
            "Indicador de fim de semana. Domínio permitido: true para domingo ou sábado; "
            "false para segunda a sexta-feira."
        ),
    },
    "dim_customer": {
        "customer_unique_id": (
            "Identificador persistente anonimizado do cliente. Chave de negócio da dimensão."
        ),
        "lifetime_order_count": (
            "Quantidade histórica de pedidos associados ao cliente. Domínio esperado: inteiro maior ou igual a 1."
        ),
        "lifetime_gmv": (
            "GMV histórico acumulado do cliente, calculado como soma de preço e frete. "
            "Domínio esperado: decimal maior ou igual a zero."
        ),
        "lifetime_avg_ticket": (
            "Ticket médio histórico do cliente, calculado como média de GMV por pedido. "
            "Domínio esperado: decimal maior ou igual a zero."
        ),
        "first_order_timestamp": "Data e hora da primeira compra conhecida do cliente.",
        "last_order_timestamp": "Data e hora da compra mais recente conhecida do cliente.",
        "customer_city": "Cidade padronizada mais representativa disponível para o cliente.",
        "customer_state": (
            "UF padronizada associada ao cliente. Domínio esperado: siglas de UFs brasileiras."
        ),
        "customer_zip_code_prefix": (
            "Prefixo de CEP de cinco dígitos do cliente; representa localização aproximada."
        ),
        "is_repeat_customer": (
            "Indicador de recorrência. Domínio permitido: true quando lifetime_order_count é maior que 1; "
            "false quando é igual a 1."
        ),
        "customer_value_band": (
            "Faixa de valor histórico do cliente baseada em lifetime_gmv. Domínio permitido: "
            "under_100, 100_to_299, 300_to_599 ou 600_or_more."
        ),
    },
    "dim_location": {
        "location_role": (
            "Papel da localização no processo de venda. Domínio permitido: customer ou seller."
        ),
        "zip_code_prefix": (
            "Prefixo de CEP de cinco dígitos da localização; representa área aproximada."
        ),
        "city": "Cidade associada à localização aproximada.",
        "state": "UF associada à localização. Domínio esperado: siglas de UFs brasileiras.",
        "latitude": (
            "Latitude aproximada da localização, agregada por prefixo de CEP. Não representa endereço individual."
        ),
        "longitude": (
            "Longitude aproximada da localização, agregada por prefixo de CEP. Não representa endereço individual."
        ),
    },
    "dim_product": {
        "product_id": "Identificador técnico do produto. Chave de negócio da dimensão.",
        "product_category_name": "Categoria de produto normalizada em português.",
        "product_category_name_english": (
            "Tradução em inglês da categoria de produto; not_available quando indisponível."
        ),
        "product_name_length": (
            "Quantidade de caracteres no nome do produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_description_length": (
            "Quantidade de caracteres na descrição do produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_photos_qty": (
            "Quantidade de fotos cadastradas para o produto. Domínio esperado: inteiro maior ou igual a zero."
        ),
        "product_weight_g": (
            "Peso do produto em gramas. Domínio esperado: número maior ou igual a zero."
        ),
        "product_length_cm": (
            "Comprimento do produto em centímetros. Domínio esperado: número maior ou igual a zero."
        ),
        "product_height_cm": (
            "Altura do produto em centímetros. Domínio esperado: número maior ou igual a zero."
        ),
        "product_width_cm": (
            "Largura do produto em centímetros. Domínio esperado: número maior ou igual a zero."
        ),
    },
    "dim_seller": {
        "seller_id": (
            "Identificador técnico anonimizado do seller. Chave de negócio da dimensão."
        ),
        "seller_zip_code_prefix": (
            "Prefixo de CEP de cinco dígitos do seller; representa localização aproximada."
        ),
        "seller_city": "Cidade padronizada do seller.",
        "seller_state": (
            "UF padronizada do seller. Domínio esperado: siglas de UFs brasileiras."
        ),
        "lifetime_order_count": (
            "Quantidade histórica de pedidos distintos que contêm itens do seller. "
            "Domínio esperado: inteiro maior ou igual a zero."
        ),
        "lifetime_product_revenue": (
            "Receita histórica de produtos do seller, sem frete. Domínio esperado: número maior ou igual a zero."
        ),
        "lifetime_freight_value": (
            "Valor histórico de frete associado a itens do seller. Domínio esperado: número maior ou igual a zero."
        ),
    },
    "dim_service_band": {
        "service_band_key": (
            "Chave técnica SHA-256 derivada da combinação de distance_band, "
            "delivery_performance_band e review_band."
        ),
        "distance_band": (
            "Faixa de distância aproximada. Domínio permitido: under_100_km, "
            "100_to_499_km, 500_to_999_km, 1000_to_1999_km, 2000_km_or_more ou not_available."
        ),
        "delivery_performance_band": (
            "Faixa de desempenho de entrega. Domínio permitido: late, on_time, "
            "early_3_days_or_more ou not_evaluated."
        ),
        "review_band": (
            "Faixa de satisfação. Domínio permitido: low_1_2, neutral_3, high_4_5 ou not_reviewed."
        ),
    },
    "fact_order_item_sales": {
        "order_id": "Identificador técnico do pedido. Parte da chave do grão da fato.",
        "order_item_id": (
            "Número sequencial do item no pedido. Com order_id, identifica unicamente o grão da fato."
        ),
        "purchase_date_key": (
            "Chave de data da compra no formato AAAAMMDD; relacionamento com dim_date.date_key."
        ),
        "customer_unique_id": (
            "Identificador persistente anonimizado do cliente; relacionamento com dim_customer."
        ),
        "customer_zip_code_prefix": (
            "Prefixo de CEP do cliente; localização aproximada do destino."
        ),
        "product_id": "Identificador técnico do produto; relacionamento com dim_product.",
        "seller_id": "Identificador técnico anonimizado do seller; relacionamento com dim_seller.",
        "shipping_limit_date": "Data e hora limite de envio do item.",
        "product_price": "Preço do produto no item. Domínio esperado: decimal maior ou igual a zero.",
        "freight_value": "Valor do frete do item. Domínio esperado: decimal maior ou igual a zero.",
        "item_gmv": (
            "GMV do item, calculado como product_price mais freight_value. "
            "Domínio esperado: decimal maior ou igual a zero."
        ),
        "freight_to_price_ratio": (
            "Razão entre frete e preço do item, calculada somente para preço positivo. "
            "Domínio esperado: número maior ou igual a zero; null quando preço não é positivo."
        ),
        "order_status": f"Status operacional do pedido. {status_domain_comment}",
        "is_delivery_late": boolean_domain_comment,
        "is_delivery_on_time": boolean_domain_comment,
        "review_score_avg": (
            "Média das notas válidas de avaliação associadas ao pedido. Domínio: número entre 1 e 5; "
            "null quando não há avaliação."
        ),
        "seller_customer_distance_km": (
            "Distância geográfica aproximada, em quilômetros, entre seller e cliente. "
            "Não representa rota real de transporte."
        ),
        "distance_band": (
            "Faixa de distância aproximada. Domínio permitido: under_100_km, "
            "100_to_499_km, 500_to_999_km, 1000_to_1999_km, 2000_km_or_more ou not_available."
        ),
        "sales_channel": (
            "Canal de venda. Domínio atual do MVP: marketplace_web."
        ),
    },
    "fact_order_service": {
        "order_id": "Identificador técnico único do pedido. Chave de negócio da fato.",
        "purchase_date_key": (
            "Chave de data da compra no formato AAAAMMDD; relacionamento com dim_date.date_key."
        ),
        "delivery_date_key": (
            "Chave de data da entrega no formato AAAAMMDD; relacionamento com dim_date.date_key; "
            "null quando o pedido não foi entregue."
        ),
        "customer_unique_id": (
            "Identificador persistente anonimizado do cliente; relacionamento com dim_customer."
        ),
        "customer_zip_code_prefix": (
            "Prefixo de CEP do cliente; representa localização aproximada do destino."
        ),
        "seller_id": (
            "Identificador do seller de referência; relacionamento com dim_seller. "
            "Para análises de distância, priorizar seller_count igual a 1."
        ),
        "service_band_key": (
            "Chave técnica de relacionamento com dim_service_band."
        ),
        "order_status": f"Status operacional do pedido. {status_domain_comment}",
        "order_item_count": "Quantidade de itens válidos no pedido. Domínio esperado: inteiro positivo.",
        "distinct_product_count": "Quantidade de produtos distintos no pedido. Domínio esperado: inteiro positivo.",
        "distinct_seller_count": "Quantidade de sellers distintos no pedido. Domínio esperado: inteiro positivo.",
        "product_total_value": "Soma dos preços dos itens, sem frete. Domínio esperado: decimal maior ou igual a zero.",
        "freight_total_value": "Soma dos fretes dos itens. Domínio esperado: decimal maior ou igual a zero.",
        "gmv_total_value": "GMV do pedido, calculado como produto mais frete. Domínio esperado: decimal maior ou igual a zero.",
        "payment_total_value": "Total de pagamentos válidos do pedido. Domínio esperado: decimal maior ou igual a zero.",
        "payment_record_count": "Quantidade de registros de pagamento válidos do pedido.",
        "max_payment_installments": "Maior quantidade de parcelas do pedido. Domínio esperado: inteiro maior ou igual a zero.",
        "payment_types": (
            "Modalidades de pagamento do pedido concatenadas por '|'. Valores possíveis por modalidade: "
            "boleto, credit_card, debit_card, voucher e not_defined."
        ),
        "review_score_avg": (
            "Média de notas válidas do pedido. Domínio: número entre 1 e 5; null sem avaliação."
        ),
        "review_count": "Quantidade de avaliações válidas do pedido.",
        "actual_delivery_days": (
            "Prazo real de entrega em dias entre compra e entrega ao cliente; null quando não entregue."
        ),
        "delivery_delay_days": (
            "Dias de atraso; diferença positiva entre entrega real e estimada, zero no prazo; "
            "null quando não avaliável."
        ),
        "delivery_early_days": (
            "Dias de antecedência; diferença positiva entre estimativa e entrega real, zero em atraso; "
            "null quando não avaliável."
        ),
        "is_delivered": (
            "Indicador de pedido entregue. Domínio: true quando status é delivered; false nos demais status."
        ),
        "is_delivery_late": (
            "Indicador de atraso. Domínio: true atrasado; false no prazo; null quando não avaliável."
        ),
        "is_delivery_on_time": (
            "Indicador de entrega no prazo. Domínio: true no prazo; false atrasado; null quando não avaliável."
        ),
        "seller_count": "Quantidade de sellers distintos no pedido.",
        "seller_customer_distance_km": (
            "Distância aproximada, em quilômetros, entre seller de referência e cliente; não representa rota real."
        ),
        "distance_band": (
            "Faixa de distância. Domínio: under_100_km, 100_to_499_km, 500_to_999_km, "
            "1000_to_1999_km, 2000_km_or_more ou not_available."
        ),
        "delivery_performance_band": (
            "Faixa de desempenho de entrega. Domínio: late, on_time, early_3_days_or_more ou not_evaluated."
        ),
        "review_band": (
            "Faixa de avaliação. Domínio: low_1_2, neutral_3, high_4_5 ou not_reviewed."
        ),
        "sales_channel": "Canal de venda. Domínio atual do MVP: marketplace_web.",
    },
    "mart_customer_experience": {
        "distance_band": (
            "Faixa de distância aproximada. Domínio: under_100_km, 100_to_499_km, "
            "500_to_999_km, 1000_to_1999_km, 2000_km_or_more ou not_available."
        ),
        "is_delivery_late": (
            "Condição de atraso para agregação. Domínio: true, false ou null quando não avaliável."
        ),
        "delivered_order_count": "Quantidade de pedidos entregues no grupo.",
        "avg_delivery_days": "Prazo médio de entrega, em dias, no grupo.",
        "avg_freight_value": "Valor médio de frete dos pedidos do grupo.",
        "avg_review_score": "Nota média de avaliação dos pedidos do grupo. Domínio: número entre 1 e 5.",
        "low_review_rate_pct": (
            "Percentual de pedidos com avaliação média menor ou igual a 2 no grupo. "
            "Domínio esperado: número entre 0 e 100."
        ),
    },
    "mart_logistics_performance": {
        "seller_id": "Identificador técnico anonimizado do seller; chave do mart.",
        "delivered_order_count": "Quantidade de pedidos entregues pelo seller.",
        "gmv_total_value": "GMV total dos pedidos entregues do seller.",
        "avg_delivery_days": "Prazo médio de entrega em dias do seller.",
        "late_delivery_rate_pct": (
            "Percentual de pedidos entregues com atraso do seller. Domínio esperado: número entre 0 e 100."
        ),
        "avg_delay_days": "Média de dias de atraso dos pedidos do seller.",
        "avg_freight_value": "Valor médio de frete dos pedidos entregues do seller.",
        "avg_review_score": "Nota média de avaliação dos pedidos entregues do seller. Domínio: 1 a 5.",
        "low_review_rate_pct": (
            "Percentual de pedidos com avaliação baixa, nota média menor ou igual a 2. "
            "Domínio esperado: número entre 0 e 100."
        ),
    },
    "mart_monthly_operations": {
        "purchase_year_month": (
            "Mês de compra no formato AAAA-MM; chave de agregação temporal do mart."
        ),
        "order_count": "Quantidade de pedidos distintos realizados no mês.",
        "gmv_total_value": "GMV total dos pedidos realizados no mês.",
        "avg_ticket_value": "Ticket médio dos pedidos realizados no mês.",
        "avg_delivery_days": "Prazo médio de entrega em dias dos pedidos do mês.",
        "late_delivery_rate_pct": (
            "Percentual de pedidos com atraso no mês. Domínio esperado: número entre 0 e 100."
        ),
        "avg_review_score": "Nota média das avaliações associadas a pedidos do mês. Domínio: 1 a 5.",
    },
    "mart_service_risk": {
        "seller_id": "Identificador técnico anonimizado do seller; chave do mart.",
        "delivered_order_count": "Quantidade de pedidos entregues pelo seller.",
        "gmv_total_value": "GMV total dos pedidos entregues do seller.",
        "avg_delivery_days": "Prazo médio de entrega em dias do seller.",
        "late_delivery_rate_pct": (
            "Percentual de pedidos entregues com atraso. Domínio esperado: número entre 0 e 100."
        ),
        "avg_delay_days": "Média de dias de atraso dos pedidos do seller.",
        "avg_freight_value": "Valor médio de frete dos pedidos do seller.",
        "avg_review_score": "Nota média das avaliações dos pedidos do seller. Domínio: 1 a 5.",
        "low_review_rate_pct": (
            "Percentual de avaliações baixas, nota média menor ou igual a 2. Domínio: 0 a 100."
        ),
        "service_risk_level": (
            "Classificação de risco de serviço. Domínio permitido: low, medium ou high. "
            "É calculada com base em taxa de atraso e nota média."
        ),
        "priority_rank": (
            "Posição de priorização do seller no mart de risco. Inteiro positivo; "
            "quanto menor o valor, maior a prioridade de análise."
        ),
    },
}

print("Dicionário de comentários das colunas Gold criado.")

# COMMAND ----------

# DBTITLE 1,Aplicar comments
all_column_docs = {
    "bronze": bronze_column_docs,
    "silver": silver_column_docs,
    "gold": gold_column_docs,
}

existing_columns_df = spark.sql(
    f"""
    SELECT
        table_schema AS schema_name,
        table_name,
        column_name
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    """
)

existing_columns = {
    (row["schema_name"], row["table_name"], row["column_name"])
    for row in existing_columns_df.collect()
}

column_documentation_results = []

for schema_name, table_docs in all_column_docs.items():
    for table_name, column_docs in table_docs.items():
        for column_name, description in column_docs.items():
            if (schema_name, table_name, column_name) in existing_columns:
                try:
                    comment_column(
                        schema_name,
                        table_name,
                        column_name,
                        description,
                    )

                    column_documentation_results.append(
                        (schema_name, table_name, column_name, "APPLIED", None)
                    )
                except Exception as error:
                    column_documentation_results.append(
                        (
                            schema_name,
                            table_name,
                            column_name,
                            "ERROR",
                            str(error),
                        )
                    )
            else:
                column_documentation_results.append(
                    (
                        schema_name,
                        table_name,
                        column_name,
                        "NOT_FOUND",
                        "Coluna não encontrada no INFORMATION_SCHEMA.",
                    )
                )

for row in existing_columns_df.collect():
    schema_name = row["schema_name"]
    table_name = row["table_name"]
    column_name = row["column_name"]

    if column_name in common_column_comments:
        try:
            comment_column(
                schema_name,
                table_name,
                column_name,
                common_column_comments[column_name],
            )

            column_documentation_results.append(
                (schema_name, table_name, column_name, "APPLIED_COMMON", None)
            )
        except Exception as error:
            column_documentation_results.append(
                (
                    schema_name,
                    table_name,
                    column_name,
                    "ERROR_COMMON",
                    str(error),
                )
            )

column_documentation_schema = StructType(
    [
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

column_documentation_df = spark.createDataFrame(
    column_documentation_results,
    schema=column_documentation_schema,
)

display(
    column_documentation_df.orderBy(
        "schema_name",
        "table_name",
        "column_name",
        "status",
    )
)

errors = (
    column_documentation_df
    .filter(F.col("status").startswith("ERROR"))
    .count()
)

assert errors == 0, (
    "Falha ao aplicar um ou mais comentários. Consulte a coluna error_message."
)

print("STATUS: APROVADO — comentários de colunas aplicados.")

# COMMAND ----------

# DBTITLE 1,Aplicar tags relevantes em colunas
column_tag_definitions = {
    "bronze": {
        "bronze_customers": {
            "customer_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "customer_unique_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "customer_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
        },
        "bronze_sellers": {
            "seller_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "seller_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
        },
        "bronze_geolocation": {
            "geolocation_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "geolocation_lat": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "geolocation_lng": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
        },
        "bronze_order_items": {
            "price": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "freight_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
        "bronze_order_payments": {
            "payment_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
    },
    "silver": {
        "silver_order_service": {
            "customer_unique_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "customer_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "primary_seller_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "seller_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "seller_latitude_geo": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "seller_longitude_geo": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "customer_latitude_geo": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "customer_longitude_geo": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "product_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "freight_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "gmv_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "payment_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
        "silver_order_items": {
            "price": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "freight_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
        "silver_geolocation_zip_agg": {
            "zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "latitude": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "longitude": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
        },
    },
    "gold": {
        "fact_order_service": {
            "customer_unique_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "customer_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "seller_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "product_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "freight_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "gmv_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "payment_total_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
        "fact_order_item_sales": {
            "customer_unique_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "customer_zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "seller_id": {
                "data_semantic_type": "anonymized_identifier",
                "sensitivity": "confidential",
            },
            "product_price": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "freight_value": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
            "item_gmv": {
                "data_semantic_type": "financial_metric",
                "sensitivity": "internal",
            },
        },
        "dim_location": {
            "zip_code_prefix": {
                "data_semantic_type": "approximate_location",
                "sensitivity": "restricted",
            },
            "latitude": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
            "longitude": {
                "data_semantic_type": "geospatial_coordinate",
                "sensitivity": "restricted",
            },
        },
    },
}

column_tag_results = []

for schema_name, tables in column_tag_definitions.items():
    for table_name, columns in tables.items():
        for column_name, tags in columns.items():
            try:
                set_column_tags(
                    schema_name,
                    table_name,
                    column_name,
                    tags,
                )
                column_tag_results.append(
                    (schema_name, table_name, column_name, "APPLIED", None)
                )
            except Exception as error:
                column_tag_results.append(
                    (
                        schema_name,
                        table_name,
                        column_name,
                        "ERROR",
                        str(error),
                    )
                )

column_tag_schema = StructType(
    [
        StructField("schema_name", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("column_name", StringType(), False),
        StructField("status", StringType(), False),
        StructField("error_message", StringType(), True),
    ]
)

column_tag_df = spark.createDataFrame(
    column_tag_results,
    schema=column_tag_schema,
)

display(column_tag_df.orderBy("schema_name", "table_name", "column_name"))

tag_errors = column_tag_df.filter(F.col("status") == "ERROR").count()

if tag_errors == 0:
    print("STATUS: APROVADO — tags aplicadas às colunas relevantes.")
else:
    print(
        "ATENÇÃO: alguns tags não foram aplicados. "
        "Consulte a coluna error_message antes de continuar."
    )

# COMMAND ----------

# DBTITLE 1,Validação de cobertura de comentários
comment_coverage_df = spark.sql(
    f"""
    SELECT
        table_schema AS schema_name,
        table_name,
        COUNT(*) AS total_columns,
        SUM(
            CASE
                WHEN comment IS NOT NULL AND TRIM(comment) <> ''
                THEN 1
                ELSE 0
            END
        ) AS documented_columns,
        ROUND(
            100.0 * SUM(
                CASE
                    WHEN comment IS NOT NULL AND TRIM(comment) <> ''
                    THEN 1
                    ELSE 0
                END
            ) / COUNT(*),
            2
        ) AS documentation_coverage_pct
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
    """
)

display(comment_coverage_df)

undocumented_columns_df = spark.sql(
    f"""
    SELECT
        table_schema AS schema_name,
        table_name,
        column_name,
        data_type
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
      AND (comment IS NULL OR TRIM(comment) = '')
    ORDER BY table_schema, table_name, ordinal_position
    """
)

undocumented_count = undocumented_columns_df.count()

print(f"Colunas sem comentário: {undocumented_count}")

if undocumented_count > 0:
    display(undocumented_columns_df)
    raise AssertionError(
        "Cobertura de comentários incompleta. Consulte a lista de colunas sem comentário."
    )

print(
    "STATUS: APROVADO — 100% das colunas Bronze, Silver e Gold "
    "possuem comentários no Unity Catalog."
)

# COMMAND ----------

# DBTITLE 1,Inventário final do catálogo
final_inventory_df = spark.sql(
    f"""
    SELECT
        table_schema AS schema_name,
        table_name,
        column_name,
        data_type,
        is_nullable AS nullable,
        comment,
        ordinal_position
    FROM {CATALOG}.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    ORDER BY
        table_schema,
        table_name,
        ordinal_position
    """
)

display(final_inventory_df)