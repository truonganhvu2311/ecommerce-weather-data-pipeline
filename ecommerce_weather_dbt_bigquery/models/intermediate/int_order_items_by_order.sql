
SELECT
    order_id,

    COUNT(*) AS item_count,

    COUNT(DISTINCT product_id)
        AS distinct_product_count,

    SUM(quantity) AS total_quantity,

    SUM(quantity * unit_price)
        AS total_items_amount

FROM {{ ref('stg_order_items') }}

GROUP BY order_id
