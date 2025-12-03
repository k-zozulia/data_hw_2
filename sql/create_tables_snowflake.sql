-- ============================================================================
-- SNOWFLAKE SCHEMA - DDL (with snow_ prefix)
-- ============================================================================

-- Drop existing tables (reverse order due to FK dependencies)
DROP TABLE IF EXISTS snow_fact_orders CASCADE;
DROP TABLE IF EXISTS snow_dim_date CASCADE;
DROP TABLE IF EXISTS snow_dim_cities CASCADE;
DROP TABLE IF EXISTS snow_dim_states CASCADE;
DROP TABLE IF EXISTS snow_dim_products CASCADE;
DROP TABLE IF EXISTS snow_dim_brands CASCADE;
DROP TABLE IF EXISTS snow_dim_categories CASCADE;
DROP TABLE IF EXISTS snow_dim_users CASCADE;
DROP TABLE IF EXISTS snow_dim_user_roles CASCADE;

-- ============================================================================
-- SUB-DIMENSION TABLES (fully normalized)
-- ============================================================================

-- Sub-dimension: User Roles
CREATE TABLE snow_dim_user_roles (
    role_id SERIAL PRIMARY KEY,
    role_name VARCHAR(50) NOT NULL UNIQUE,
    role_description TEXT
);

-- Sub-dimension: Categories
CREATE TABLE snow_dim_categories (
    category_id SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL,
    category_slug VARCHAR(100) UNIQUE NOT NULL,
    parent_category_id INTEGER REFERENCES snow_dim_categories(category_id)
);

-- Sub-dimension: Brands
CREATE TABLE snow_dim_brands (
    brand_id SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE,
    brand_country VARCHAR(100),
    brand_website VARCHAR(255)
);

-- Sub-dimension: States
CREATE TABLE snow_dim_states (
    state_id SERIAL PRIMARY KEY,
    state_name VARCHAR(100) NOT NULL,
    state_code VARCHAR(10) NOT NULL UNIQUE,
    region VARCHAR(50),
    country VARCHAR(100) DEFAULT 'United States'
);

-- Sub-dimension: Cities
CREATE TABLE snow_dim_cities (
    city_id SERIAL PRIMARY KEY,
    city_name VARCHAR(100) NOT NULL,
    state_id INTEGER REFERENCES snow_dim_states(state_id),
    population INTEGER,
    timezone VARCHAR(50),
    UNIQUE(city_name, state_id)
);

-- ============================================================================
-- MAIN DIMENSION TABLES (reference sub-dimensions)
-- ============================================================================

-- Dimension: Users (normalized)
CREATE TABLE snow_dim_users (
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
    role_id INTEGER REFERENCES snow_dim_user_roles(role_id),

    -- Address components (normalized to sub-dimensions)
    city_id INTEGER REFERENCES snow_dim_cities(city_id),
    postal_code VARCHAR(20),
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- Dimension: Products (normalized)
CREATE TABLE snow_dim_products (
    product_id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    category_id INTEGER REFERENCES snow_dim_categories(category_id),
    brand_id INTEGER REFERENCES snow_dim_brands(brand_id),
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
CREATE TABLE snow_dim_date (
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

-- ============================================================================
-- FACT TABLE
-- ============================================================================

CREATE TABLE snow_fact_orders (
    fact_id SERIAL PRIMARY KEY,
    order_id INTEGER NOT NULL,
    user_id INTEGER NOT NULL REFERENCES snow_dim_users(user_id),
    product_id INTEGER NOT NULL REFERENCES snow_dim_products(product_id),
    date_id INTEGER NOT NULL REFERENCES snow_dim_date(date_id),

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

-- Sub-dimension indexes
CREATE INDEX idx_snow_dim_categories_parent ON snow_dim_categories(parent_category_id);
CREATE INDEX idx_snow_dim_categories_slug ON snow_dim_categories(category_slug);
CREATE INDEX idx_snow_dim_brands_name ON snow_dim_brands(brand_name);
CREATE INDEX idx_snow_dim_states_code ON snow_dim_states(state_code);
CREATE INDEX idx_snow_dim_states_region ON snow_dim_states(region);
CREATE INDEX idx_snow_dim_cities_state ON snow_dim_cities(state_id);
CREATE INDEX idx_snow_dim_cities_name ON snow_dim_cities(city_name);

-- Main dimension indexes
CREATE INDEX idx_snow_dim_users_email ON snow_dim_users(email);
CREATE INDEX idx_snow_dim_users_username ON snow_dim_users(username);
CREATE INDEX idx_snow_dim_users_role ON snow_dim_users(role_id);
CREATE INDEX idx_snow_dim_users_city ON snow_dim_users(city_id);

CREATE INDEX idx_snow_dim_products_category ON snow_dim_products(category_id);
CREATE INDEX idx_snow_dim_products_brand ON snow_dim_products(brand_id);
CREATE INDEX idx_snow_dim_products_price ON snow_dim_products(price);
CREATE INDEX idx_snow_dim_products_sku ON snow_dim_products(sku);

CREATE INDEX idx_snow_dim_date_full_date ON snow_dim_date(full_date);
CREATE INDEX idx_snow_dim_date_year_month ON snow_dim_date(year, month);
CREATE INDEX idx_snow_dim_date_year_quarter ON snow_dim_date(year, quarter);

-- Fact table indexes
CREATE INDEX idx_snow_fact_orders_user_id ON snow_fact_orders(user_id);
CREATE INDEX idx_snow_fact_orders_product_id ON snow_fact_orders(product_id);
CREATE INDEX idx_snow_fact_orders_date_id ON snow_fact_orders(date_id);
CREATE INDEX idx_snow_fact_orders_order_id ON snow_fact_orders(order_id);

-- Composite indexes for snowflake queries
CREATE INDEX idx_snow_fact_orders_date_product ON snow_fact_orders(date_id, product_id);
CREATE INDEX idx_snow_fact_orders_date_user ON snow_fact_orders(date_id, user_id);
CREATE INDEX idx_snow_fact_orders_user_product ON snow_fact_orders(user_id, product_id);

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE snow_dim_user_roles IS 'Snowflake: User role lookup table';
COMMENT ON TABLE snow_dim_categories IS 'Snowflake: Product categories (supports hierarchy)';
COMMENT ON TABLE snow_dim_brands IS 'Snowflake: Product brands';
COMMENT ON TABLE snow_dim_states IS 'Snowflake: US State/province lookup with regions';
COMMENT ON TABLE snow_dim_cities IS 'Snowflake: City lookup (normalized to states)';
COMMENT ON TABLE snow_dim_users IS 'Snowflake: User dimension (normalized to sub-dimensions)';
COMMENT ON TABLE snow_dim_products IS 'Snowflake: Product dimension (normalized to categories and brands)';
COMMENT ON TABLE snow_dim_date IS 'Snowflake: Date dimension with pre-calculated attributes';
COMMENT ON TABLE snow_fact_orders IS 'Snowflake: Fact table - order transactions';

-- ============================================================================
-- SAMPLE DATA FOR SUB-DIMENSIONS
-- ============================================================================

-- Insert sample user roles
INSERT INTO snow_dim_user_roles (role_name, role_description) VALUES
    ('user', 'Regular user'),
    ('admin', 'Administrator'),
    ('moderator', 'Moderator'),
    ('guest', 'Guest user')
ON CONFLICT (role_name) DO NOTHING;
