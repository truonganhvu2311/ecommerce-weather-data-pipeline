
{{ config(materialized='view') }}

WITH order_items AS (

    SELECT
        order_item_id,
        order_id,
        product_id,
        quantity,
        unit_price,
        created_at,
        updated_at

    FROM {{ ref('stg_order_items') }}

),

eligible_orders AS (

    SELECT
        order_id,
        customer_id,
        order_date_vn,
        city,
        status

    FROM {{ ref('fct_orders') }}

),

final AS (

    SELECT
        i.order_item_id,
        i.order_id,
        i.product_id,

        o.customer_id,
        o.order_date_vn,
        o.city,
        o.status,

        i.quantity,
        i.unit_price,

        -- Gia tri cua tung dong san pham
        i.quantity * i.unit_price AS line_amount,

        i.created_at,
        i.updated_at

    FROM order_items i

    INNER JOIN eligible_orders o
        ON i.order_id = o.order_id

)

SELECT *
FROM final
