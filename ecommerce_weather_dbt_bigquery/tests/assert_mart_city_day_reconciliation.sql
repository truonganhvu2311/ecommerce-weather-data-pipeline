
WITH source_totals AS (

    SELECT
        COUNT(*) AS total_orders,

        COALESCE(SUM(total_amount), 0)
            AS total_amount

    FROM {{ ref('fct_orders') }}

),

mart_totals AS (

    SELECT
        COALESCE(SUM(order_count), 0)
            AS total_orders,

        COALESCE(SUM(total_order_amount), 0)
            AS total_amount

    FROM {{ ref('mart_city_day') }}

)

SELECT
    s.total_orders AS source_orders,
    m.total_orders AS mart_orders,
    s.total_amount AS source_amount,
    m.total_amount AS mart_amount

FROM source_totals s
CROSS JOIN mart_totals m

WHERE
    s.total_orders <> m.total_orders
    OR s.total_amount IS DISTINCT FROM m.total_amount
