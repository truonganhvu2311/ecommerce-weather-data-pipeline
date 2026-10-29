
{{ config(materialized='view') }}

WITH order_items AS (

    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price

    FROM {{ ref('stg_order_items') }}

),

aggregated AS (

    SELECT
        order_id,

        COUNT(*) AS item_count,

        COUNT(DISTINCT product_id)
            AS distinct_product_count,

        SUM(quantity)
            AS total_quantity,

        SUM(quantity * unit_price)
            AS total_items_amount

    FROM order_items

    GROUP BY order_id

)

SELECT *
FROM aggregated
