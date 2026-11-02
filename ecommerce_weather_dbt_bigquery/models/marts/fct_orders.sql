
WITH eligible_orders AS (

    SELECT *
    FROM {{ ref('int_orders_eligible') }}

),

order_items_summary AS (

    SELECT
        order_id,
        item_count,
        distinct_product_count,
        total_quantity,
        total_items_amount
    FROM {{ ref('int_order_items_by_order') }}

),

final AS (

    SELECT
        o.*,

        COALESCE(i.item_count, 0)
            AS item_count,

        COALESCE(i.distinct_product_count, 0)
            AS distinct_product_count,

        COALESCE(i.total_quantity, 0)
            AS total_quantity,

        COALESCE(i.total_items_amount, 0)
            AS total_items_amount,

        (i.order_id IS NOT NULL)
            AS has_order_items

    FROM eligible_orders AS o

    LEFT JOIN order_items_summary AS i
        ON o.order_id = i.order_id

)

SELECT *
FROM final
