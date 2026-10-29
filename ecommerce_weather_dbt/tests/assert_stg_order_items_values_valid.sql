
SELECT
    order_item_id,
    quantity,
    unit_price

FROM {{ ref('stg_order_items') }}

WHERE
    quantity <= 0
    OR unit_price < 0
