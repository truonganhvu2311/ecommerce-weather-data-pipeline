
SELECT
    order_id,
    status,
    is_deleted

FROM {{ ref('int_orders_eligible') }}

WHERE
    status NOT IN ('paid', 'shipped', 'delivered')
    OR is_deleted IS DISTINCT FROM FALSE
