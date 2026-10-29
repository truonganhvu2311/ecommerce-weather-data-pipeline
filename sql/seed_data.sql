
-- 1. THEM KHACH HANG
INSERT INTO customers (customer_id, customer_name, city)
VALUES
    ('C001', 'Nguyen An', 'Hanoi'),
    ('C002', 'Tran Binh', 'Ho Chi Minh City'),
    ('C003', 'Le Chi', 'Da Nang'),
    ('C004', 'Pham Dung', 'Hanoi'),
    ('C005', 'Hoang Giang', 'Ho Chi Minh City')
ON CONFLICT (customer_id) DO NOTHING;


-- 2. THEM SAN PHAM
INSERT INTO products (
    product_id, product_name, category, price
)
VALUES
    ('P001', 'Running Shoes', 'Footwear', 2500000),
    ('P002', 'Basic T-Shirt', 'Apparel', 350000),
    ('P003', 'Backpack', 'Accessories', 800000),
    ('P004', 'Cap', 'Accessories', 200000),
    ('P005', 'Jacket', 'Apparel', 1200000),
    ('P006', 'Sports Socks', 'Accessories', 150000)
ON CONFLICT (product_id) DO NOTHING;


-- 3. THEM DON HANG
INSERT INTO orders (
    order_id, customer_id, order_date,
    status, city, total_amount
)
VALUES
    ('O001', 'C001', '2026-10-01 09:30:00+07',
     'delivered', 'Hanoi', 3200000),

    ('O002', 'C002', '2026-10-02 11:00:00+07',
     'paid', 'Ho Chi Minh City', 1000000),

    ('O003', 'C003', '2026-10-02 14:00:00+07',
     'shipped', 'Da Nang', 1350000),

    ('O004', 'C004', '2026-10-03 16:20:00+07',
     'delivered', 'Hanoi', 1600000),

    ('O005', 'C002', '2026-10-04 10:10:00+07',
     'cancelled', 'Ho Chi Minh City', 2500000),

    ('O006', 'C005', '2026-10-05 17:45:00+07',
     'paid', 'Ho Chi Minh City', 1150000),

    ('O007', 'C003', '2026-10-06 09:00:00+07',
     'pending', 'Da Nang', 1350000),

    ('O008', 'C001', '2026-10-06 19:15:00+07',
     'delivered', 'Hanoi', 2700000)
ON CONFLICT (order_id) DO NOTHING;


-- 4. THEM TUNG DONG SAN PHAM TRONG DON HANG
INSERT INTO order_items (
    order_item_id, order_id, product_id,
    quantity, unit_price
)
VALUES
    ('OI001', 'O001', 'P001', 1, 2500000),
    ('OI002', 'O001', 'P002', 2, 350000),

    ('OI003', 'O002', 'P003', 1, 800000),
    ('OI004', 'O002', 'P004', 1, 200000),

    ('OI005', 'O003', 'P002', 3, 350000),
    ('OI006', 'O003', 'P006', 2, 150000),

    ('OI007', 'O004', 'P005', 1, 1200000),
    ('OI008', 'O004', 'P004', 2, 200000),

    ('OI009', 'O005', 'P001', 1, 2500000),

    ('OI010', 'O006', 'P003', 1, 800000),
    ('OI011', 'O006', 'P002', 1, 350000),

    ('OI012', 'O007', 'P005', 1, 1200000),
    ('OI013', 'O007', 'P006', 1, 150000),

    ('OI014', 'O008', 'P001', 1, 2500000),
    ('OI015', 'O008', 'P004', 1, 200000)
ON CONFLICT (order_item_id) DO NOTHING;

COMMENT ON COLUMN customers.city IS
'Thành phố trong hồ sơ khách hàng';

COMMENT ON COLUMN orders.city IS
'Thành phố giao hàng của đơn, được ghi nhận tại thời điểm đặt hàng';