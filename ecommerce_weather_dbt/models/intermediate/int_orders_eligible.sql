
{{ config(materialized='view') }}

WITH eligible_orders AS (

    SELECT
        order_id,
        customer_id,
        order_date,

        -- Chuyen timestamp sang gio Viet Nam
        order_date AT TIME ZONE 'Asia/Ho_Chi_Minh'
            AS order_datetime_vn,

        -- Lay ngay theo gio Viet Nam de JOIN weather
        (order_date AT TIME ZONE 'Asia/Ho_Chi_Minh')::date
            AS order_date_vn,

        status,
        city,
        total_amount,
        created_at,
        updated_at,
        is_deleted,
        extracted_at,
        source_raw_event_id

    FROM {{ ref('stg_orders') }}

    WHERE status IN ('paid', 'shipped', 'delivered')
      AND is_deleted IS FALSE

)

SELECT *
FROM eligible_orders
