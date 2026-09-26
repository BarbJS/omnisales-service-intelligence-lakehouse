-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 — Análise de Negócio
-- MAGIC
-- MAGIC ## OmniSales Service Intelligence Lakehouse
-- MAGIC
-- MAGIC Este notebook apresenta a análise de negócio do MVP **OmniSales Service Intelligence Lakehouse**. A análise utiliza tabelas da camada Gold, previamente validadas na etapa de qualidade de dados, para responder às perguntas de negócio relacionadas a desempenho comercial, sellers, logística, frete, distância, satisfação, risco de serviço e recorrência de clientes.
-- MAGIC
-- MAGIC ### Objetivos
-- MAGIC
-- MAGIC - Consolidar indicadores executivos de pedidos, clientes, sellers, GMV, ticket, frete, entrega, atraso e avaliação.
-- MAGIC - Identificar tendências mensais de desempenho comercial e operacional.
-- MAGIC - Avaliar categorias e sellers com maior relevância para o negócio.
-- MAGIC - Medir a associação entre distância, frete, atrasos e satisfação.
-- MAGIC - Identificar sellers com maior risco de serviço.
-- MAGIC - Analisar perfil, valor e recorrência de clientes.
-- MAGIC - Consolidar recomendações acionáveis baseadas nos resultados.
-- MAGIC
-- MAGIC ### Fontes utilizadas
-- MAGIC
-- MAGIC - Fatos: `fact_order_service` e `fact_order_item_sales`.
-- MAGIC - Marts: `mar_monthly_operations`, `mart_logistic_performance`, `mart_service_risk` e `mart_customer_experience`.
-- MAGIC - Dimensões: `dim_costumer`, `dim_product`, `dim_seller` e `dim_date`.
-- MAGIC
-- MAGIC > Observação: o nome da dimensão de clientes foi mantido como `dim_costumer`, conforme o nome físico criado na camada Gold.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Perguntas de negócio do MVP
-- MAGIC
-- MAGIC 1. Quais categorias, sellers e estados concentram maior número de pedidos, receita de produtos, valor de frete e ticket médio?
-- MAGIC 2. Qual é a taxa de entrega no prazo, o atraso médio e o prazo médio de entrega por seller, categoria e estado do cliente?
-- MAGIC 3. Pedidos entregues após a data estimada apresentam avaliações piores do que pedidos entregues no prazo?
-- MAGIC 4. Como a distância aproximada entre seller e cliente se relaciona com prazo de entrega, atraso e valor de frete?
-- MAGIC 5. Quais sellers ou categorias combinam alta receita com baixa confiabilidade logística e baixa satisfação?
-- MAGIC 6. Quais estados ou cidades possuem alta concentração de pedidos, mas desempenho logístico abaixo da média?
-- MAGIC 7. Existe associação entre a proporção frete/preço do produto e avaliações baixas?
-- MAGIC 8. Em quais meses há crescimento de pedidos acompanhado de piora de atraso, prazo ou satisfação?
-- MAGIC 9. Quais perfis de cliente apresentam recorrência e gasto mais elevados, e como foi sua experiência de entrega?
-- MAGIC 10. Quais meios de pagamento e faixas de parcelamento estão associados a maior ticket médio, mantendo a análise separada da qualidade logística?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Contexto executivo
-- MAGIC
-- MAGIC Antes de responder às perguntas de negócio, esta seção apresenta indicadores consolidados da base. Ela não constitui uma pergunta do MVP, mas fornece referência para interpretar as análises seguintes.

-- COMMAND ----------

-- DBTITLE 1,KPIs gerais
SELECT
    COUNT(DISTINCT order_id) AS total_orders,
    COUNT(DISTINCT customer_unique_id) AS total_customers,
    COUNT(DISTINCT seller_id) AS total_reference_sellers,
    ROUND(SUM(gmv_total_value), 2) AS gmv_total_value,
    ROUND(AVG(gmv_total_value), 2) AS avg_ticket_value,
    ROUND(SUM(product_total_value), 2) AS product_revenue_value,
    ROUND(SUM(freight_total_value), 2) AS freight_total_value,
    ROUND(AVG(freight_total_value), 2) AS avg_freight_value,
    ROUND(AVG(actual_delivery_days), 2) AS avg_delivery_days,
    ROUND(
        100.0 * AVG(
            CASE
                WHEN is_delivery_on_time = true THEN 1.0
                WHEN is_delivery_on_time = false THEN 0.0
                ELSE NULL
            END
        ),
        2
    ) AS on_time_delivery_rate_pct,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
    ROUND(AVG(review_score_avg), 2) AS avg_review_score
FROM main_catalog.gold.fact_order_service;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 1 — Quais categorias, sellers e estados concentram maior número de pedidos, receita de produtos, valor de frete e ticket médio?
-- MAGIC
-- MAGIC A análise é apresentada em três recortes: categoria de produto, seller e estado do cliente. Para evitar duplicidade de valores, o recorte por categoria e seller utiliza a fato no grão de item; já o recorte por estado do cliente utiliza a fato no grão de pedido.

-- COMMAND ----------

-- DBTITLE 1,Categorias
SELECT
    COALESCE(p.product_category_name, 'not_available') AS analysis_group,
    'category' AS analysis_level,
    COUNT(DISTINCT i.order_id) AS order_count,
    COUNT(*) AS item_count,
    ROUND(SUM(i.product_price), 2) AS product_revenue_value,
    ROUND(SUM(i.freight_value), 2) AS freight_total_value,
    ROUND(SUM(i.item_gmv), 2) AS gmv_total_value,
    ROUND(AVG(i.item_gmv), 2) AS avg_ticket_value
FROM main_catalog.gold.fact_order_item_sales i
LEFT JOIN main_catalog.gold.dim_product p
    ON i.product_id = p.product_id
GROUP BY
    COALESCE(p.product_category_name, 'not_available')
ORDER BY
    product_revenue_value DESC
LIMIT 5;

-- COMMAND ----------

-- DBTITLE 1,sellers
SELECT
    i.seller_id AS analysis_group,
    'seller' AS analysis_level,
    COUNT(DISTINCT i.order_id) AS order_count,
    COUNT(*) AS item_count,
    ROUND(SUM(i.product_price), 2) AS product_revenue_value,
    ROUND(SUM(i.freight_value), 2) AS freight_total_value,
    ROUND(SUM(i.item_gmv), 2) AS gmv_total_value,
    ROUND(AVG(i.item_gmv), 2) AS avg_ticket_value
FROM main_catalog.gold.fact_order_item_sales i
GROUP BY
    i.seller_id
ORDER BY
    product_revenue_value DESC
LIMIT 20;

-- COMMAND ----------

-- DBTITLE 1,estados de cliente
SELECT
    c.customer_state AS analysis_group,
    'customer_state' AS analysis_level,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.product_total_value), 2) AS product_revenue_value,
    ROUND(SUM(f.freight_total_value), 2) AS freight_total_value,
    ROUND(SUM(f.gmv_total_value), 2) AS gmv_total_value,
    ROUND(AVG(f.gmv_total_value), 2) AS avg_ticket_value
FROM main_catalog.gold.fact_order_service f
LEFT JOIN main_catalog.gold.dim_customer c
    ON f.customer_unique_id = c.customer_unique_id
GROUP BY
    c.customer_state
ORDER BY
    product_revenue_value DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 2 — Qual é a taxa de entrega no prazo, o atraso médio e o prazo médio de entrega por seller, categoria e estado do cliente?
-- MAGIC
-- MAGIC A taxa de entrega no prazo considera somente pedidos entregues e avaliáveis. A análise por seller e estado do cliente utiliza o grão de pedido; a análise por categoria utiliza o grão de item, pois um pedido pode conter produtos de mais de uma categoria.

-- COMMAND ----------

-- DBTITLE 1,desempenho por seller
SELECT
    l.seller_id AS analysis_group,
    'seller' AS analysis_level,
    l.delivered_order_count,
    ROUND(100.0 - l.late_delivery_rate_pct, 2) AS on_time_delivery_rate_pct,
    ROUND(l.avg_delay_days, 2) AS avg_delay_days,
    ROUND(l.avg_delivery_days, 2) AS avg_delivery_days,
    ROUND(l.avg_review_score, 2) AS avg_review_score
FROM main_catalog.gold.mart_logistics_performance l
WHERE l.delivered_order_count >= 20
ORDER BY
    on_time_delivery_rate_pct ASC,
    l.delivered_order_count DESC
LIMIT 30;

-- COMMAND ----------

-- DBTITLE 1,desempenho por categoria
SELECT
    COALESCE(p.product_category_name, 'not_available') AS analysis_group,
    'category' AS analysis_level,
    COUNT(DISTINCT i.order_id) AS delivered_order_count,
    ROUND(
        100.0 * AVG(
            CASE
                WHEN i.is_delivery_on_time = true THEN 1.0
                WHEN i.is_delivery_on_time = false THEN 0.0
                ELSE NULL
            END
        ),
        2
    ) AS on_time_delivery_rate_pct,
    ROUND(AVG(i.seller_customer_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(i.review_score_avg), 2) AS avg_review_score
FROM main_catalog.gold.fact_order_item_sales i
LEFT JOIN main_catalog.gold.dim_product p
    ON i.product_id = p.product_id
WHERE i.order_status = 'delivered'
GROUP BY
    COALESCE(p.product_category_name, 'not_available')
HAVING COUNT(DISTINCT i.order_id) >= 20
ORDER BY
    on_time_delivery_rate_pct ASC,
    delivered_order_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Limitação analítica no recorte por categoria
-- MAGIC
-- MAGIC A tabela `fact_order_item_sales` está no grão de item de pedido e contém a categoria do produto, a condição de entrega no prazo (`is_delivery_on_time`) e a avaliação média associada ao pedido. Por isso, ela permite calcular, por categoria, a quantidade de pedidos entregues, a taxa de entrega no prazo e a avaliação média.
-- MAGIC
-- MAGIC Entretanto, as métricas `actual_delivery_days` e `delivery_delay_days` foram modeladas na tabela `fact_order_service`, cujo grão é um pedido. Um mesmo pedido pode conter itens de categorias diferentes. Dessa forma, uma junção direta entre as duas tabelas para calcular prazo médio e atraso médio por categoria replicaria as métricas de um pedido para cada item ou categoria associada, podendo duplicar valores e distorcer a análise.
-- MAGIC
-- MAGIC Para preservar a integridade do grão analítico, o MVP apresenta prazo médio e atraso médio por seller e por estado do cliente, mas limita o recorte por categoria à taxa de entrega no prazo e à avaliação média. Essa limitação está documentada e pode ser tratada em uma evolução futura por meio de uma regra explícita de atribuição logística por item, categoria predominante ou pedido com categoria única.

-- COMMAND ----------

-- DBTITLE 1,desempenho por estado do cliente
SELECT
    c.customer_state AS analysis_group,
    'customer_state' AS analysis_level,
    COUNT(DISTINCT f.order_id) AS delivered_order_count,
    ROUND(
        100.0 * AVG(
            CASE
                WHEN f.is_delivery_on_time = true THEN 1.0
                WHEN f.is_delivery_on_time = false THEN 0.0
                ELSE NULL
            END
        ),
        2
    ) AS on_time_delivery_rate_pct,
    ROUND(AVG(f.delivery_delay_days), 2) AS avg_delay_days,
    ROUND(AVG(f.actual_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(f.review_score_avg), 2) AS avg_review_score
FROM main_catalog.gold.fact_order_service f
LEFT JOIN main_catalog.gold.dim_customer c
    ON f.customer_unique_id = c.customer_unique_id
WHERE f.is_delivered = true
GROUP BY
    c.customer_state
HAVING COUNT(DISTINCT f.order_id) >= 20
ORDER BY
    on_time_delivery_rate_pct ASC,
    delivered_order_count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 3 — Pedidos entregues após a data estimada apresentam avaliações piores do que pedidos entregues no prazo?
-- MAGIC
-- MAGIC A comparação é realizada somente para pedidos entregues e com avaliação disponível. São comparadas a avaliação média, a proporção de avaliações baixas e o volume de pedidos entre entregas no prazo e entregas atrasadas.

-- COMMAND ----------

SELECT
    CASE
        WHEN is_delivery_late = true THEN 'late'
        WHEN is_delivery_on_time = true THEN 'on_time'
        ELSE 'not_evaluated'
    END AS delivery_status,
    COUNT(DISTINCT order_id) AS delivered_order_count,
    ROUND(AVG(actual_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
    ROUND(AVG(review_score_avg), 2) AS avg_review_score,
    ROUND(
        100.0 * AVG(
            CASE
                WHEN review_score_avg <= 2 THEN 1.0
                WHEN review_score_avg IS NOT NULL THEN 0.0
                ELSE NULL
            END
        ),
        2
    ) AS low_review_rate_pct
FROM main_catalog.gold.fact_order_service
WHERE
    is_delivered = true
    AND review_score_avg IS NOT NULL
    AND is_delivery_late IS NOT NULL
GROUP BY
    CASE
        WHEN is_delivery_late = true THEN 'late'
        WHEN is_delivery_on_time = true THEN 'on_time'
        ELSE 'not_evaluated'
    END
ORDER BY delivery_status;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 4 — Como a distância aproximada entre seller e cliente se relaciona com prazo de entrega, atraso e valor de frete?
-- MAGIC
-- MAGIC A análise considera pedidos entregues com somente um seller, porque a distância disponível na fato de serviço é baseada no seller de referência. As métricas são avaliadas por faixa de distância aproximada.

-- COMMAND ----------

SELECT
    distance_band,
    COUNT(DISTINCT order_id) AS delivered_order_count,
    ROUND(AVG(seller_customer_distance_km), 2) AS avg_distance_km,
    ROUND(AVG(freight_total_value), 2) AS avg_freight_value,
    ROUND(AVG(actual_delivery_days), 2) AS avg_delivery_days,
    ROUND(AVG(delivery_delay_days), 2) AS avg_delay_days,
    ROUND(
        100.0 * AVG(
            CASE
                WHEN is_delivery_late = true THEN 1.0
                WHEN is_delivery_late = false THEN 0.0
                ELSE NULL
            END
        ),
        2
    ) AS late_delivery_rate_pct,
    ROUND(AVG(review_score_avg), 2) AS avg_review_score
FROM main_catalog.gold.fact_order_service
WHERE
    is_delivered = true
    AND seller_count = 1
    AND distance_band <> 'not_available'
GROUP BY distance_band
ORDER BY
    CASE distance_band
        WHEN 'under_100_km' THEN 1
        WHEN '100_to_499_km' THEN 2
        WHEN '500_to_999_km' THEN 3
        WHEN '1000_to_1999_km' THEN 4
        WHEN '2000_km_or_more' THEN 5
        ELSE 6
    END;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Pergunta 5 — Quais sellers ou categorias combinam alta receita com baixa confiabilidade logística e baixa satisfação?
-- MAGIC
-- MAGIC Para sellers, a análise utiliza a mart logística, que contém receita de pedidos entregues, taxa de atraso e avaliação. Para categorias, a fato de itens permite medir receita, taxa de entrega no prazo e avaliação, mas não possui prazo ou atraso médio no grão de categoria.

-- COMMAND ----------

-- DBTITLE 1,sellers prioritários
WITH seller_metrics AS (
    SELECT
        seller_id,
        delivered_order_count,
        gmv_total_value,
        late_delivery_rate_pct,
        avg_delivery_days,
        avg_delay_days,
        avg_review_score,
        low_review_rate_pct,
        PERCENTILE_APPROX(gmv_total_value, 0.75) OVER () AS p75_gmv,
        AVG(late_delivery_rate_pct) OVER () AS overall_late_rate,
        AVG(avg_review_score) OVER () AS overall_review_score
    FROM main_catalog.gold.mart_logistics_performance
    WHERE delivered_order_count >= 20
)
SELECT
    seller_id,
    delivered_order_count,
    ROUND(gmv_total_value, 2) AS gmv_total_value,
    ROUND(late_delivery_rate_pct, 2) AS late_delivery_rate_pct,
    ROUND(avg_delivery_days, 2) AS avg_delivery_days,
    ROUND(avg_delay_days, 2) AS avg_delay_days,
    ROUND(avg_review_score, 2) AS avg_review_score,
    ROUND(low_review_rate_pct, 2) AS low_review_rate_pct
FROM seller_metrics
WHERE
    gmv_total_value >= p75_gmv
    AND late_delivery_rate_pct > overall_late_rate
    AND avg_review_score < overall_review_score
ORDER BY
    gmv_total_value DESC,
    late_delivery_rate_pct DESC,
    avg_review_score ASC
LIMIT 10

-- COMMAND ----------

-- DBTITLE 1,categorias prioritárias
WITH category_metrics AS (
    SELECT
        COALESCE(p.product_category_name, 'not_available') AS product_category_name,
        COUNT(DISTINCT i.order_id) AS order_count,
        SUM(i.item_gmv) AS gmv_total_value,
        100.0 * AVG(
            CASE
                WHEN i.is_delivery_late = true THEN 1.0
                WHEN i.is_delivery_late = false THEN 0.0
                ELSE NULL
            END
        ) AS late_delivery_rate_pct,
        AVG(i.review_score_avg) AS avg_review_score,
        100.0 * AVG(
            CASE
                WHEN i.review_score_avg <= 2 THEN 1.0
                WHEN i.review_score_avg IS NOT NULL THEN 0.0
                ELSE NULL
            END
        ) AS low_review_rate_pct
    FROM main_catalog.gold.fact_order_item_sales i
    LEFT JOIN main_catalog.gold.dim_product p
        ON i.product_id = p.product_id
    GROUP BY
        COALESCE(p.product_category_name, 'not_available')
    HAVING COUNT(DISTINCT i.order_id) >= 20
),
benchmarks AS (
    SELECT
        PERCENTILE_APPROX(gmv_total_value, 0.75) AS p75_gmv,
        AVG(late_delivery_rate_pct) AS overall_late_rate,
        AVG(avg_review_score) AS overall_review_score
    FROM category_metrics
)
SELECT
    c.product_category_name,
    c.order_count,
    ROUND(c.gmv_total_value, 2) AS gmv_total_value,
    ROUND(c.late_delivery_rate_pct, 2) AS late_delivery_rate_pct,
    ROUND(c.avg_review_score, 2) AS avg_review_score,
    ROUND(c.low_review_rate_pct, 2) AS low_review_rate_pct
FROM category_metrics c
CROSS JOIN benchmarks b
WHERE
    c.gmv_total_value >= b.p75_gmv
    AND c.late_delivery_rate_pct > b.overall_late_rate
    AND c.avg_review_score < b.overall_review_score
ORDER BY
    c.gmv_total_value DESC,
    c.late_delivery_rate_pct DESC,
    c.avg_review_score ASC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC # Síntese final da análise de negócio
-- MAGIC
-- MAGIC A análise integrada da camada Gold confirmou que a organização pode relacionar desempenho comercial, logística e satisfação do cliente ao longo de toda a jornada do pedido. A consolidação das informações de pedidos, itens, sellers, localização, frete, entrega e avaliações permitiu identificar onde está concentrada a receita e quais segmentos comerciais exigem atenção por apresentarem risco operacional ou experiência inferior.
-- MAGIC
-- MAGIC ## 1. Concentração comercial por categoria, seller e estado
-- MAGIC
-- MAGIC A demanda e a receita estão fortemente concentradas no estado de São Paulo. O estado concentrou 41.746 pedidos, R$ 5,20 milhões em receita de produtos, R$ 718,8 mil em frete e R$ 5,92 milhões em GMV, com ticket médio de R$ 143,13. Rio de Janeiro e Minas Gerais aparecem na sequência, com 12.856 e 11.629 pedidos, respectivamente. Embora São Paulo concentre o maior volume absoluto, estados com menor volume apresentaram tickets médios mais elevados, como Paraíba (R$ 264,97), Acre (R$ 242,84), Amapá (R$ 239,16), Alagoas (R$ 234,13) e Rondônia (R$ 233,46). Isso sugere que a estratégia comercial deve combinar escala nas regiões de maior volume com monitoramento de rentabilidade e custo logístico nas regiões de ticket mais alto.
-- MAGIC
-- MAGIC Entre as categorias, `beleza_saude` apresentou o maior GMV, de R$ 1,44 milhão, seguida por `relogios_presentes`, com R$ 1,31 milhão, e `cama_mesa_banho`, com R$ 1,24 milhão. A categoria `cama_mesa_banho` apresentou o maior número de pedidos entre as principais categorias, com 9.417 pedidos, enquanto `beleza_saude` registrou 8.836 pedidos. Em ticket médio por item, destacaram-se `relogios_presentes` (R$ 217,92) e `moveis_escritorio` (R$ 202,56), indicando categorias de maior valor comercial por venda. [574]
-- MAGIC
-- MAGIC A concentração por seller também é relevante. O seller `4869f7a5dfa277a7dca6462dcf3b52b2` liderou a receita de produtos entre os sellers analisados, com R$ 229,47 mil em 1.132 pedidos. Outros sellers de destaque incluem `53243585a1d6dc2643021fd1853d8905`, com ticket médio de R$ 575,26, e `7e93a43ef30c4f03f38b393420bc753a`, com ticket médio de R$ 537,51. Esses sellers operam com volume menor que os líderes por quantidade de pedidos, mas concentram pedidos de maior valor e, por isso, merecem acompanhamento comercial e operacional individualizado. [575]
-- MAGIC
-- MAGIC ## 2. Desempenho logístico por seller, categoria e estado
-- MAGIC
-- MAGIC O desempenho logístico apresentou diferença relevante entre os estados. São Paulo, que concentra o maior volume, registrou taxa de entrega no prazo de 94,11%, prazo médio de 8,76 dias, atraso médio de 0,41 dia e avaliação média de 4,25. Minas Gerais e Paraná também apresentaram bom desempenho, com taxas de entrega no prazo de 94,39% e 95,00%, respectivamente. [576]
-- MAGIC
-- MAGIC Por outro lado, alguns estados apresentaram desempenho mais frágil. Alagoas registrou a menor taxa de entrega no prazo, de 76,07%, com prazo médio de 24,54 dias e avaliação média de 3,86. Maranhão teve 80,45% de entregas no prazo e avaliação média de 3,82. Ceará, Sergipe, Bahia e Rio de Janeiro também ficaram abaixo de 86,6% de entregas no prazo. Rio de Janeiro merece atenção prioritária porque combina desempenho inferior, com 86,52% de entregas no prazo e 1,73 dia de atraso médio, a um volume expressivo de 12.353 pedidos entregues. [576]
-- MAGIC
-- MAGIC No recorte por categoria, `cama_mesa_banho`, `beleza_saude`, `informatica_acessorios`, `moveis_decoracao` e `relogios_presentes` apresentaram volumes elevados e taxas de entrega no prazo entre 90,95% e 93,25%. Algumas categorias relevantes apresentaram avaliação média abaixo de 4, como `moveis_escritorio` (3,51), `cama_mesa_banho` (3,93), `moveis_decoracao` (3,96), `informatica_acessorios` (3,99) e `telefonia` (3,99), o que indica oportunidade de investigação mesmo quando a taxa de entrega no prazo não é a pior da base. [577]
-- MAGIC
-- MAGIC A análise por categoria possui uma limitação intencional do modelo: a tabela no grão de item permite calcular a taxa de entrega no prazo e a avaliação associada, mas prazo médio de entrega e atraso médio são métricas armazenadas no grão de pedido. Como um pedido pode conter itens de mais de uma categoria, associar diretamente esses tempos a categorias duplicaria métricas de pedido. Por esse motivo, prazo médio e atraso médio foram preservados nos recortes por seller e por estado do cliente, onde o grão de pedido é mantido.
-- MAGIC
-- MAGIC No nível de seller, há heterogeneidade operacional importante. Entre os sellers com pelo menos 20 pedidos entregues, foram observadas taxas de entrega no prazo inferiores a 70%, como `f76a3b1349b6df1ee875d1f3fa4340f0` (60,87%), `821fb029fc6e495ca4f08a35d51e53a5` (62,50%), `ede0c03645598cdfc63ca8237acbe73d` (65,12%) e `ad781527c93d00d89a11eecd9dcad7c1` (65,71%). Alguns sellers também apresentaram atrasos médios elevados, como `2a1348e9addc1af5aaa619b1a3679d6b`, com 12,58 dias, e `054694fa03fe82cec4b7551487331d74`, com 10,72 dias. [578]
-- MAGIC
-- MAGIC ## 3. Relação entre atraso e satisfação do cliente
-- MAGIC
-- MAGIC A comparação entre pedidos entregues no prazo e pedidos entregues após a data estimada demonstrou uma diferença expressiva na experiência do cliente. Os pedidos entregues com atraso totalizaram 7.613 ocorrências e apresentaram prazo médio de entrega de 31,38 dias, atraso médio de 9,45 dias, avaliação média de 2,57 e taxa de avaliações baixas de 54,00%.
-- MAGIC
-- MAGIC Em contraste, os 87.751 pedidos entregues no prazo apresentaram prazo médio de 10,87 dias, atraso médio nulo, avaliação média de 4,39 e somente 3,16% de avaliações baixas. A avaliação média dos pedidos atrasados foi 1,82 ponto inferior à observada nos pedidos entregues no prazo, enquanto a proporção de avaliações baixas foi aproximadamente 17 vezes maior.
-- MAGIC
-- MAGIC Os resultados indicam uma forte associação entre atraso de entrega e deterioração da experiência pós-venda. Embora a análise não permita atribuir causalidade exclusiva ao atraso, a diferença simultânea de prazo, avaliação média e incidência de avaliações baixas evidencia que a confiabilidade logística deve ser tratada como prioridade de negócio para proteção da satisfação do cliente.
-- MAGIC
-- MAGIC ## 4. Sellers e categorias prioritários
-- MAGIC
-- MAGIC A análise de priorização identificou categorias de alta relevância comercial que combinam receita expressiva, atraso acima da referência e satisfação abaixo da média. As categorias `relogios_presentes`, `cama_mesa_banho`, `informatica_acessorios`, `moveis_decoracao`, `bebes`, `telefonia` e `moveis_escritorio` foram classificadas como prioritárias. [580]
-- MAGIC
-- MAGIC Entre elas, `cama_mesa_banho` requer atenção por combinar 9.417 pedidos, R$ 1,24 milhão em GMV, taxa de atraso de 8,40%, avaliação média de 3,90 e 18,68% de avaliações baixas. `moveis_decoracao` apresentou R$ 902,5 mil de GMV, taxa de atraso de 8,43% e 19,24% de avaliações baixas. O caso mais crítico é `moveis_escritorio`: embora possua volume menor, com 1.273 pedidos e R$ 342,5 mil de GMV, apresentou avaliação média de 3,49 e 26,17% de avaliações baixas, além de taxa de atraso de 8,93%. [580]
-- MAGIC
-- MAGIC No recorte de sellers, o seller `7c67e1448b00f6e969d365cea6b010ab` merece prioridade por combinar R$ 237,94 mil de GMV, 968 pedidos entregues, prazo médio de 22,42 dias, taxa de atraso de 10,12%, avaliação média de 3,50 e 25,00% de avaliações baixas. O seller `4a3ca9315b744ce9f8e9374361493884` apresentou R$ 234,62 mil de GMV em 1.724 pedidos, com taxa de atraso de 11,19% e avaliação média de 3,89. [581]
-- MAGIC
-- MAGIC Também foram identificados sellers com risco operacional mais severo, ainda que com menor GMV. O seller `712e6ed8aa4aa1fa65dab41fed5737e4` apresentou 22,08% de atrasos, 24,69 dias de prazo médio, 4,46 dias de atraso médio, avaliação média de 3,39 e 35,06% de avaliações baixas. O seller `2eb70248d66e0e3ef83659f71b244378` apresentou avaliação média de 2,79 e 46,77% de avaliações baixas. Esses casos devem ser tratados como prioridade de investigação, pois combinam falhas logísticas e forte risco para a experiência do cliente. [581]
-- MAGIC
-- MAGIC ## Conclusão
-- MAGIC
-- MAGIC Os resultados demonstram que o Lakehouse atingiu o objetivo analítico do MVP: tornou possível analisar faturamento, frete, sellers, localização, entrega e avaliação de forma integrada. A empresa passa a identificar não apenas onde está o maior volume comercial, mas também onde esse volume convive com risco de atraso, prazos longos e satisfação menor.
-- MAGIC
-- MAGIC ## Recomendações de negócio
-- MAGIC
-- MAGIC 1. Priorizar a redução de atrasos de entrega como principal alavanca de experiência do cliente. Pedidos entregues com atraso apresentaram avaliação média de 2,57, contra 4,39 nos pedidos entregues no prazo, e taxa de avaliações baixas de 54,00%, contra 3,16%.
-- MAGIC
-- MAGIC 2. Priorizar planos de ação para sellers de alta receita com taxa de atraso elevada e avaliação abaixo da média, começando pelos sellers `7c67e1448b00f6e969d365cea6b010ab` e `4a3ca9315b744ce9f8e9374361493884`.
-- MAGIC
-- MAGIC 3. Investigar as categorias `cama_mesa_banho`, `moveis_decoracao`, `informatica_acessorios` e `moveis_escritorio`, pois combinam relevância comercial com sinais de menor confiabilidade ou satisfação.
-- MAGIC
-- MAGIC 4. Monitorar especificamente a operação destinada a Rio de Janeiro, Bahia, Ceará, Maranhão, Sergipe e Alagoas. Rio de Janeiro deve receber atenção imediata devido ao alto volume de pedidos e ao desempenho logístico inferior aos principais estados de referência.
-- MAGIC
-- MAGIC 5. Criar indicadores recorrentes de frete médio, prazo médio, atraso médio, taxa de atraso e avaliação média por faixa de distância. As faixas acima de 1.000 km apresentaram custo logístico e risco de atraso superiores, além de avaliações médias menores.
-- MAGIC
-- MAGIC 6. Criar uma rotina de acompanhamento dos sellers de risco, utilizando GMV, volume entregue, taxa de atraso, atraso médio, avaliação média e percentual de avaliações baixas como critérios de priorização.
-- MAGIC
-- MAGIC 7. Manter as análises no nível correto de granularidade: métricas financeiras e de categoria devem utilizar a fato de itens; métricas de prazo e atraso devem utilizar a fato de pedidos ou marts de seller; e as conclusões devem evitar somar indicadores de tabelas com grãos diferentes.