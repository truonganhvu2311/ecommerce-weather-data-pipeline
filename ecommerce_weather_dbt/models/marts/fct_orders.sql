
{{ config(materialized='view') }}

WITH eligible_orders AS (

    SELECT
        order_id,
        customer_id,
        order_date,
        order_datetime_vn,
        order_date_vn,
        status,
        city,
        total_amount,
        is_deleted,
        created_at,
        updated_at,
        extracted_at,
        source_raw_event_id

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
        o.order_id,
        o.customer_id,

        o.order_date,
        o.order_datetime_vn,
        o.order_date_vn,

        o.status,
        o.city,

        -- Gia tri don hang tu bang orders
        o.total_amount,

        -- Thong tin tong hop tu order_items
        i.item_count,
        i.distinct_product_count,
        i.total_quantity,
        i.total_items_amount,

        -- Kiem tra don hang co order_items hay khong
        (i.order_id IS NOT NULL) AS has_order_items,

        o.is_deleted,
        o.created_at,
        o.updated_at,
        o.extracted_at,
        o.source_raw_event_id

    FROM eligible_orders o

    LEFT JOIN order_items_summary i
        ON o.order_id = i.order_id

)

SELECT *
FROM final
