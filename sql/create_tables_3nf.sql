-- ============================================================================
-- 3NF (THIRD NORMAL FORM) SCHEMA - DDL
-- ============================================================================

-- Drop existing tables (reverse order due to FK dependencies)
DROP TABLE IF EXISTS order_items CASCADE;
DROP TABLE IF EXISTS orders CASCADE;
DROP TABLE IF EXISTS reviews CASCADE;
DROP TABLE IF EXISTS product_images CASCADE;
DROP TABLE IF EXISTS product_tags CASCADE;
DROP TABLE IF EXISTS products CASCADE;
DROP TABLE IF EXISTS categories CASCADE;
DROP TABLE IF EXISTS users CASCADE;
DROP TABLE IF EXISTS companies CASCADE;
DROP TABLE IF EXISTS banks CASCADE;
DROP TABLE IF EXISTS addresses CASCADE;

-- ============================================================================
-- INDEPENDENT TABLES (no foreign keys)
-- ============================================================================

-- Table: addresses
-- Stores all address information (for users and companies)
CREATE TABLE addresses (
    id SERIAL PRIMARY KEY,
    address_line VARCHAR(255),
    city VARCHAR(100),
    state VARCHAR(100),
    state_code VARCHAR(10),
    postal_code VARCHAR(20),
    country VARCHAR(100) DEFAULT 'United States',
    latitude DECIMAL(10, 8),
    longitude DECIMAL(11, 8)
);

-- Table: banks
-- Stores bank card and financial information
CREATE TABLE banks (
    id SERIAL PRIMARY KEY,
    card_number VARCHAR(20),
    card_type VARCHAR(50),
    card_expire VARCHAR(10),
    currency VARCHAR(10),
    iban VARCHAR(50)
);

-- Table: categories
-- Product categories (independent lookup table)
CREATE TABLE categories (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    slug VARCHAR(100) UNIQUE NOT NULL
);

-- ============================================================================
-- TABLES WITH DEPENDENCIES
-- ============================================================================

-- Table: companies
-- Depends on: addresses
CREATE TABLE companies (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    department VARCHAR(100),
    title VARCHAR(100),
    address_id INTEGER REFERENCES addresses(id) ON DELETE SET NULL
);

-- Table: users
-- Depends on: addresses, banks, companies
CREATE TABLE users (
    id INTEGER PRIMARY KEY,
    first_name VARCHAR(100),
    last_name VARCHAR(100),
    maiden_name VARCHAR(100),
    age INTEGER,
    gender VARCHAR(20),
    email VARCHAR(255),
    phone VARCHAR(50),
    username VARCHAR(100),
    password VARCHAR(255),
    birth_date DATE,
    image_url TEXT,
    blood_group VARCHAR(10),
    height DECIMAL(5, 2),
    weight DECIMAL(5, 2),
    eye_color VARCHAR(50),
    hair_color VARCHAR(50),
    hair_type VARCHAR(50),
    ip_address VARCHAR(50),
    mac_address VARCHAR(50),
    user_agent TEXT,
    university VARCHAR(255),
    ein VARCHAR(20),
    ssn VARCHAR(20),
    role VARCHAR(50) DEFAULT 'user',
    crypto_coin VARCHAR(50),
    crypto_wallet TEXT,
    crypto_network VARCHAR(50),
    bank_id INTEGER REFERENCES banks(id) ON DELETE SET NULL,
    company_id INTEGER REFERENCES companies(id) ON DELETE SET NULL,
    address_id INTEGER REFERENCES addresses(id) ON DELETE SET NULL
);

-- Table: products
-- Depends on: categories
CREATE TABLE products (
    id INTEGER PRIMARY KEY,
    title VARCHAR(255) NOT NULL,
    description TEXT,
    category_id INTEGER REFERENCES categories(id) ON DELETE SET NULL,
    price DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    rating DECIMAL(3, 2),
    stock INTEGER,
    brand VARCHAR(100),
    sku VARCHAR(100),
    weight DECIMAL(10, 2),
    width DECIMAL(10, 2),
    height DECIMAL(10, 2),
    depth DECIMAL(10, 2),
    warranty_info TEXT,
    shipping_info TEXT,
    availability_status VARCHAR(100),
    return_policy TEXT,
    minimum_order_quantity INTEGER,
    barcode VARCHAR(100),
    qr_code_url TEXT,
    thumbnail_url TEXT,
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Table: product_tags
-- Depends on: products (many-to-many relationship via tags)
CREATE TABLE product_tags (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    tag VARCHAR(100) NOT NULL
);

-- Table: product_images
-- Depends on: products
CREATE TABLE product_images (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    image_url TEXT NOT NULL,
    image_order INTEGER DEFAULT 0
);

-- Table: reviews
-- Depends on: products
CREATE TABLE reviews (
    id SERIAL PRIMARY KEY,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    rating INTEGER CHECK (rating >= 1 AND rating <= 5),
    comment TEXT,
    reviewer_name VARCHAR(255),
    reviewer_email VARCHAR(255),
    review_date TIMESTAMP
);

-- Table: orders
-- Depends on: users
CREATE TABLE orders (
    id INTEGER PRIMARY KEY,
    user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
    order_date TIMESTAMP NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    total DECIMAL(10, 2),
    discounted_total DECIMAL(10, 2),
    total_products INTEGER,
    total_quantity INTEGER
);

-- Table: order_items
-- Depends on: orders, products (junction table)
CREATE TABLE order_items (
    id SERIAL PRIMARY KEY,
    order_id INTEGER REFERENCES orders(id) ON DELETE CASCADE,
    product_id INTEGER REFERENCES products(id) ON DELETE CASCADE,
    quantity INTEGER NOT NULL,
    price DECIMAL(10, 2),
    discount_percentage DECIMAL(5, 2),
    discounted_total DECIMAL(10, 2),
    total DECIMAL(10, 2)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Users indexes
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_username ON users(username);
CREATE INDEX idx_users_bank ON users(bank_id);
CREATE INDEX idx_users_company ON users(company_id);
CREATE INDEX idx_users_address ON users(address_id);

-- Products indexes
CREATE INDEX idx_products_category ON products(category_id);
CREATE INDEX idx_products_brand ON products(brand);
CREATE INDEX idx_products_price ON products(price);
CREATE INDEX idx_products_stock ON products(stock);
CREATE INDEX idx_products_rating ON products(rating);

-- Reviews indexes
CREATE INDEX idx_reviews_product ON reviews(product_id);
CREATE INDEX idx_reviews_rating ON reviews(rating);

-- Orders indexes
CREATE INDEX idx_orders_user ON orders(user_id);
CREATE INDEX idx_orders_date ON orders(order_date);
CREATE INDEX idx_orders_status ON orders(status);

-- Order items indexes
CREATE INDEX idx_order_items_order ON order_items(order_id);
CREATE INDEX idx_order_items_product ON order_items(product_id);

-- Product tags indexes
CREATE INDEX idx_product_tags_product ON product_tags(product_id);
CREATE INDEX idx_product_tags_tag ON product_tags(tag);

-- Product images indexes
CREATE INDEX idx_product_images_product ON product_images(product_id);

-- Companies indexes
CREATE INDEX idx_companies_address ON companies(address_id);

-- Addresses indexes
CREATE INDEX idx_addresses_city ON addresses(city);
CREATE INDEX idx_addresses_state ON addresses(state_code);
CREATE INDEX idx_addresses_postal ON addresses(postal_code);