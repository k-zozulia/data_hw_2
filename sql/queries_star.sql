-- ============================================================================
-- SQL QUERIES FOR STAR SCHEMA
-- ============================================================================
-- These queries demonstrate analytics on dimensional model
-- Fast aggregations with simple JOINs
-- ============================================================================

-- ============================================================================
-- 1. TOTAL REVENUE BY PRODUCT
-- ============================================================================
-- Aggregate sales metrics per product

SELECT
    p.product_id,
    p.title,
    p.category,
    p.brand,
    p.price AS current_price,

    -- Aggregations from fact table
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.subtotal) AS total_subtotal,
    SUM(f.discount_amount) AS total_discounts_given,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.unit_price), 2) AS avg_selling_price,

    -- Performance metrics
    ROUND(SUM(f.total_amount) / NULLIF(SUM(f.quantity), 0), 2) AS revenue_per_unit

FROM star_fact_orders f
JOIN star_dim_products p ON f.product_id = p.product_id

GROUP BY
    p.product_id, p.title, p.category,
    p.brand, p.price

ORDER BY total_revenue DESC
LIMIT 50;


-- ============================================================================
-- 2. TOP USERS BY TOTAL SPEND
-- ============================================================================
-- Identify highest-value customers

SELECT
    u.user_id,
    u.username,
    u.full_name,
    u.email,
    u.age,
    u.gender,
    u.role,

    -- Purchase statistics
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items_purchased,

    -- Revenue metrics
    SUM(f.total_amount) AS lifetime_value,
    SUM(f.discount_amount) AS total_discounts_received,
    ROUND(AVG(f.total_amount), 2) AS avg_transaction_value,

    -- Time metrics
    COUNT(DISTINCT f.date_id) AS days_with_purchases,
    MIN(d.full_date) AS first_purchase_date,
    MAX(d.full_date) AS last_purchase_date

FROM star_fact_orders f
JOIN star_dim_users u ON f.user_id = u.user_id
JOIN star_dim_date d ON f.date_id = d.date_id

GROUP BY
    u.user_id, u.username, u.full_name,
    u.email, u.age, u.gender, u.role

ORDER BY lifetime_value DESC
LIMIT 100;


-- ============================================================================
-- 3. REVENUE BY TIME PERIOD (DAILY/WEEKLY/MONTHLY)
-- ============================================================================

-- Daily Revenue
SELECT
    d.full_date,
    d.day_name,
    d.is_weekend,

    COUNT(DISTINCT f.order_id) AS orders_count,
    SUM(f.quantity) AS items_sold,
    SUM(f.total_amount) AS daily_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM star_fact_orders f
JOIN star_dim_date d ON f.date_id = d.date_id

GROUP BY d.full_date, d.day_name, d.is_weekend
ORDER BY d.full_date DESC
LIMIT 30;


-- Weekly Revenue
SELECT
    d.year,
    d.week_of_year,
    MIN(d.full_date) AS week_start,
    MAX(d.full_date) AS week_end,

    COUNT(DISTINCT f.order_id) AS orders_count,
    SUM(f.quantity) AS items_sold,
    SUM(f.total_amount) AS weekly_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM star_fact_orders f
JOIN star_dim_date d ON f.date_id = d.date_id

GROUP BY d.year, d.week_of_year
ORDER BY d.year DESC, d.week_of_year DESC
LIMIT 20;


-- Monthly Revenue
SELECT
    d.year,
    d.month,
    d.month_name,
    d.quarter,

    COUNT(DISTINCT f.order_id) AS orders_count,
    COUNT(DISTINCT f.user_id) AS unique_customers,
    SUM(f.quantity) AS items_sold,
    SUM(f.total_amount) AS monthly_revenue,
    SUM(f.discount_amount) AS total_discounts,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,

    -- Growth metrics
    LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month) AS prev_month_revenue,
    ROUND(
        (SUM(f.total_amount) - LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month))
        / NULLIF(LAG(SUM(f.total_amount)) OVER (ORDER BY d.year, d.month)), 0) * 100,
        2
    ) AS revenue_growth_percent

FROM star_fact_orders f
JOIN star_dim_date d ON f.date_id = d.date_id

GROUP BY d.year, d.month, d.month_name, d.quarter
ORDER BY d.year DESC, d.month DESC;


-- ============================================================================
-- 4. REVENUE BY LOCATION
-- ============================================================================
-- Geographic analysis of sales

SELECT
    l.country,
    l.region,
    l.state,
    l.city,

    -- Sales metrics
    COUNT(DISTINCT f.order_id) AS orders_count,
    COUNT(DISTINCT f.user_id) AS unique_customers,
    SUM(f.quantity) AS items_sold,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value,

    -- Customer metrics
    ROUND(SUM(f.total_amount) / NULLIF(COUNT(DISTINCT f.user_id), 0), 2) AS revenue_per_customer

FROM star_fact_orders f
JOIN star_dim_location l ON f.location_id = l.location_id

GROUP BY l.country, l.region, l.state, l.city

ORDER BY total_revenue DESC
LIMIT 100;


-- Revenue by Region Summary
SELECT
    l.region,
    COUNT(DISTINCT l.state) AS states_count,
    COUNT(DISTINCT l.city) AS cities_count,
    COUNT(DISTINCT f.user_id) AS unique_customers,
    SUM(f.total_amount) AS regional_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM star_fact_orders f
JOIN star_dim_location l ON f.location_id = l.location_id

GROUP BY l.region
ORDER BY regional_revenue DESC;


-- ============================================================================
-- 5. CATEGORY PERFORMANCE ANALYSIS
-- ============================================================================
-- Sales performance by product category

SELECT
    p.category,

    -- Product metrics
    COUNT(DISTINCT p.product_id) AS products_in_category,

    -- Sales metrics
    COUNT(DISTINCT f.order_id) AS orders_count,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.discount_amount) AS total_discounts,

    -- Averages
    ROUND(AVG(f.unit_price), 2) AS avg_product_price,
    ROUND(AVG(f.total_amount), 2) AS avg_transaction_value,

    -- Top brand in category
    (
        SELECT p2.brand
        FROM star_fact_orders f2
        JOIN star_dim_products p2 ON f2.product_id = p2.product_id
        WHERE p2.category = p.category
        GROUP BY p2.brand
        ORDER BY SUM(f2.total_amount) DESC
        LIMIT 1
    ) AS top_brand

FROM star_fact_orders f
JOIN star_dim_products p ON f.product_id = p.product_id

GROUP BY p.category

ORDER BY total_revenue DESC;
