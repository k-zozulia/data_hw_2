-- ============================================================================
-- STAR SCHEMA - DDL (with star_ prefix)
-- ============================================================================
-- This schema represents a Star Schema dimensional model
-- All tables prefixed with 'star_' to avoid conflicts with 3NF and Snowflake
-- ============================================================================

-- Drop existing tables (reverse order due to FK dependencies)
DROP TABLE IF EXISTS star_fact_orders CASCADE;
DROP TABLE IF EXISTS star_dim_date CASCADE;
DROP TABLE IF EXISTS star_dim_location CASCADE;
DROP TABLE IF EXISTS star_dim_products CASCADE;
DROP TABLE IF EXISTS star_dim_users CASCADE;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- Dimension: Users
CREATE TABLE star_dim_users (
    user_id INTEGER PRIMARY KEY,
    username VARCHAR(100),
    email VARCHAR(255),
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    full_name VARCHAR(255),
    age INTEGER,
    gender VARCHAR(20),
    phone VARCHAR(50),
    birth_date DATE,
    blood_group VARCHAR(10),
    university VARCHAR(255),
    role VARCHAR(50)
);

-- Dimension: Products
CREATE TABLE star_dim_products (
    product_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(100),
    brand VARCHAR(100),
    sku VARCHAR(100),
    price DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    rating DECIMAL(3, 2),
    stock INTEGER,
    weight DECIMAL(10, 2),
    warranty_info TEXT,
    availability_status VARCHAR(100)
);

-- Dimension: Date
CREATE TABLE star_dim_date (
    date_id INTEGER PRIMARY KEY,
    full_date DATE NOT NULL UNIQUE,
    year INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    month INTEGER NOT NULL,
    month_name VARCHAR(20),
    day INTEGER NOT NULL,
    day_of_week INTEGER NOT NULL,
    day_name VARCHAR(20),
    week_of_year INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    is_holiday BOOLEAN DEFAULT FALSE,
    fiscal_year INTEGER,
    fiscal_quarter INTEGER
);

-- Dimension: Location
CREATE TABLE star_dim_location (
    location_id SERIAL PRIMARY KEY,
    address_line VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    state_code VARCHAR(10),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'United States',
    region VARCHAR(50),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE TABLE star_fact_orders (
    fact_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL REFERENCES star_dim_users(user_id),
    product_id INTEGER NOT NULL REFERENCES star_dim_products(product_id),
    date_id INTEGER NOT NULL REFERENCES star_dim_date(date_id),
    location_id INTEGER REFERENCES star_dim_location(location_id),

    -- Measures (metrics)
    quantity INTEGER NOT NULL,
    unit_price DECIMAL(10, 2) NOT NULL,
    discount_percentage DECIMAL(5, 2) DEFAULT 0,
    discount_amount DECIMAL(10, 2) DEFAULT 0,
    subtotal DECIMAL(10, 2) NOT NULL,
    total_amount DECIMAL(10, 2) NOT NULL,

    -- Order context
    order_status VARCHAR(50),

    -- Timestamps
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Dimension indexes
CREATE INDEX idx_star_dim_users_email ON star_dim_users(email);
CREATE INDEX idx_star_dim_users_username ON star_dim_users(username);

CREATE INDEX idx_star_dim_products_category ON star_dim_products(category);
CREATE INDEX idx_star_dim_products_brand ON star_dim_products(brand);
CREATE INDEX idx_star_dim_products_price ON star_dim_products(price);

CREATE INDEX idx_star_dim_date_full_date ON star_dim_date(full_date);
CREATE INDEX idx_star_dim_date_year_month ON star_dim_date(year, month);
CREATE INDEX idx_star_dim_date_year_quarter ON star_dim_date(year, quarter);

CREATE INDEX idx_star_dim_location_city ON star_dim_location(city);
CREATE INDEX idx_star_dim_location_state ON star_dim_location(state);
CREATE INDEX idx_star_dim_location_country ON star_dim_location(country);

-- Fact table indexes (for fast aggregations)
CREATE INDEX idx_star_fact_orders_user_id ON star_fact_orders(user_id);
CREATE INDEX idx_star_fact_orders_product_id ON star_fact_orders(product_id);
CREATE INDEX idx_star_fact_orders_date_id ON star_fact_orders(date_id);
CREATE INDEX idx_star_fact_orders_location_id ON star_fact_orders(location_id);
CREATE INDEX idx_star_fact_orders_order_id ON star_fact_orders(order_id);

-- Composite indexes for common queries
CREATE INDEX idx_star_fact_orders_date_product ON star_fact_orders(date_id, product_id);
CREATE INDEX idx_star_fact_orders_date_user ON star_fact_orders(date_id, user_id);
CREATE INDEX idx_star_fact_orders_user_product ON star_fact_orders(user_id, product_id);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE star_dim_users IS 'Star Schema: User dimension - contains user attributes';
COMMENT ON TABLE star_dim_products IS 'Star Schema: Product dimension - contains product attributes';
COMMENT ON TABLE star_dim_date IS 'Star Schema: Date dimension - pre-generated date attributes';
COMMENT ON TABLE star_dim_location IS 'Star Schema: Location dimension - geographic information';
COMMENT ON TABLE star_fact_orders IS 'Star Schema: Fact table - order transactions with measures';

COMMENT ON COLUMN star_fact_orders.quantity IS 'Number of units ordered';
COMMENT ON COLUMN star_fact_orders.unit_price IS 'Price per unit';
COMMENT ON COLUMN star_fact_orders.discount_amount IS 'Total discount applied';
COMMENT ON COLUMN star_fact_orders.subtotal IS 'Quantity × Unit Price';
COMMENT ON COLUMN star_fact_orders.total_amount IS 'Subtotal - Discount Amount';