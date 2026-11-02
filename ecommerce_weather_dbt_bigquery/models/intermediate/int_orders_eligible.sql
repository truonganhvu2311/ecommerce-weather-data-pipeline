
WITH eligible_orders AS (
    SELECT
        o.*,

        DATETIME(
            o.order_date,
            'Asia/Ho_Chi_Minh'
        ) AS order_datetime_vn,

        DATE(
            o.order_date,
            'Asia/Ho_Chi_Minh'
        ) AS order_date_vn

    FROM {{ ref('stg_orders') }} AS o

    WHERE LOWER(TRIM(o.status)) IN (
        'paid',
        'shipped',
        'delivered'
    )
    AND o.is_deleted IS FALSE
)

SELECT *
FROM eligible_orders
