BEGIN;

INSERT INTO orders (
    order_id,
    customer_id,
    order_date,
    status,
    city,
    total_amount
)
VALUES (
    'O009',
    'C004',
    CURRENT_TIMESTAMP,
    'pending',
    'Hanoi',
    600000
)
ON CONFLICT (order_id) DO NOTHING;

INSERT INTO order_items (
    order_item_id,
    order_id,
    product_id,
    quantity,
    unit_price
)
VALUES (
    'OI016',
    'O009',
    'P004',
    3,
    200000
)
ON CONFLICT (order_item_id) DO NOTHING;

COMMIT;