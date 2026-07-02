-- Create Schema
CREATE SCHEMA IF NOT EXISTS gold;

-- Dimension: Date
CREATE TABLE IF NOT EXISTS gold.dim_date (
    date_key INTEGER PRIMARY KEY,  -- Format: YYYYMMDD
    full_date DATE NOT NULL,
    day_of_week INTEGER NOT NULL,  -- 0 = Monday, 6 = Sunday
    day_name VARCHAR(15) NOT NULL,
    day_of_month INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(15) NOT NULL,
    quarter INTEGER NOT NULL,
    year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL DEFAULT FALSE,
    is_month_start BOOLEAN NOT NULL DEFAULT FALSE,
    is_month_end BOOLEAN NOT NULL DEFAULT FALSE,
    is_holiday BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Customer (SCD Type 2)
CREATE TABLE IF NOT EXISTS gold.dim_customer (
    customer_key INTEGER PRIMARY KEY,  -- Surrogate key
    customer_id VARCHAR(36) NOT NULL,  -- Natural key (UUID)
    first_name VARCHAR(100) NOT NULL,
    last_name VARCHAR(100) NOT NULL,
    email VARCHAR(255),
    phone VARCHAR(50),
    address VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(50),
    segment VARCHAR(50),
    valid_from DATE NOT NULL,
    valid_to DATE NOT NULL DEFAULT '9999-12-31',
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Dimension: Product
CREATE TABLE IF NOT EXISTS gold.dim_product (
    product_key INTEGER PRIMARY KEY,  -- Surrogate key
    product_id VARCHAR(36) NOT NULL,  -- Natural key (UUID)
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100) NOT NULL,
    price DECIMAL(10, 2) NOT NULL,
    cost_price DECIMAL(10, 2) NOT NULL,
    price_range VARCHAR(50) NOT NULL,  -- Budget, Mid-Range, Premium, Luxury
    margin_percent DECIMAL(5, 2) NOT NULL,
    supplier VARCHAR(255),
    rating DECIMAL(2, 1),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Fact: Orders
CREATE TABLE IF NOT EXISTS gold.fact_orders (
    order_key INTEGER PRIMARY KEY,  -- Surrogate key
    order_id VARCHAR(36) NOT NULL,  -- Natural key (UUID)
    customer_key INTEGER NOT NULL REFERENCES gold.dim_customer(customer_key),
    product_key INTEGER NOT NULL REFERENCES gold.dim_product(product_key),
    date_key INTEGER NOT NULL REFERENCES gold.dim_date(date_key),
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_percent DECIMAL(5, 2) DEFAULT 0.0,
    discount_amount DECIMAL(10, 2) DEFAULT 0.0,
    total_amount DECIMAL(10, 2) NOT NULL,
    net_amount DECIMAL(10, 2) NOT NULL,
    shipping_cost DECIMAL(10, 2) DEFAULT 0.0,
    payment_method VARCHAR(50),
    order_status VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes for performance optimizations on joins and aggregations
CREATE INDEX IF NOT EXISTS idx_fact_orders_customer ON gold.fact_orders(customer_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_product ON gold.fact_orders(product_key);
CREATE INDEX IF NOT EXISTS idx_fact_orders_date ON gold.fact_orders(date_key);
CREATE INDEX IF NOT EXISTS idx_dim_customer_id ON gold.dim_customer(customer_id);
CREATE INDEX IF NOT EXISTS idx_dim_product_id ON gold.dim_product(product_id);
CREATE INDEX IF NOT EXISTS idx_dim_customer_current ON gold.dim_customer(is_current);
