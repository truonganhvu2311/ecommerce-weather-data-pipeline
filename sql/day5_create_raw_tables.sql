-- ==================================================
-- DAY 5 - RAW TRANSACTION TABLES
-- Database: ecommerce_analytics_dev
-- ==================================================

-- 1. CUSTOMERS

CREATE TABLE IF NOT EXISTS raw.customers (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    customer_id VARCHAR(20),
    customer_name VARCHAR(100),
    city VARCHAR(100),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,

    _source_table TEXT,
    _extracted_at TIMESTAMPTZ,
    _watermark_from TIMESTAMPTZ,
    _watermark_to TIMESTAMPTZ,

    source_file TEXT NOT NULL,
    source_line_number INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_customers_file_line
        UNIQUE (source_file, source_line_number)
);


-- 2. PRODUCTS

CREATE TABLE IF NOT EXISTS raw.products (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    product_id VARCHAR(20),
    product_name VARCHAR(150),
    category VARCHAR(80),
    price NUMERIC(12, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,

    _source_table TEXT,
    _extracted_at TIMESTAMPTZ,
    _watermark_from TIMESTAMPTZ,
    _watermark_to TIMESTAMPTZ,

    source_file TEXT NOT NULL,
    source_line_number INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_products_file_line
        UNIQUE (source_file, source_line_number)
);


-- 3. ORDERS

CREATE TABLE IF NOT EXISTS raw.orders (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    order_id VARCHAR(20),
    customer_id VARCHAR(20),
    order_date TIMESTAMPTZ,
    status VARCHAR(20),
    city VARCHAR(100),
    total_amount NUMERIC(14, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,
    is_deleted BOOLEAN,

    _source_table TEXT,
    _extracted_at TIMESTAMPTZ,
    _watermark_from TIMESTAMPTZ,
    _watermark_to TIMESTAMPTZ,

    source_file TEXT NOT NULL,
    source_line_number INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_orders_file_line
        UNIQUE (source_file, source_line_number)
);


-- 4. ORDER_ITEMS

CREATE TABLE IF NOT EXISTS raw.order_items (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    order_item_id VARCHAR(20),
    order_id VARCHAR(20),
    product_id VARCHAR(20),
    quantity INTEGER,
    unit_price NUMERIC(12, 2),
    created_at TIMESTAMPTZ,
    updated_at TIMESTAMPTZ,

    _source_table TEXT,
    _extracted_at TIMESTAMPTZ,
    _watermark_from TIMESTAMPTZ,
    _watermark_to TIMESTAMPTZ,

    source_file TEXT NOT NULL,
    source_line_number INTEGER NOT NULL,
    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT uq_raw_order_items_file_line
        UNIQUE (source_file, source_line_number)
);

CREATE TABLE IF NOT EXISTS raw.weather_api_responses (
    raw_event_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,

    source_file TEXT NOT NULL UNIQUE,

    payload JSONB NOT NULL,

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS etl.loaded_files (
    source_file TEXT PRIMARY KEY,

    source_type TEXT NOT NULL,

    sha256 CHAR(64) NOT NULL,

    record_count INTEGER NOT NULL
        CHECK (record_count >= 0),

    loaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);