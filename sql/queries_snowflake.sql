-- ============================================================================
-- SQL QUERIES FOR SNOWFLAKE SCHEMA
-- ============================================================================
-- These queries demonstrate normalized dimensional model
-- Multiple JOINs through sub-dimensions
-- ============================================================================

-- ============================================================================
-- 1. REVENUE BY PRODUCT WITH CATEGORY HIERARCHY
-- ============================================================================
-- Navigate through brand and category sub-dimensions

SELECT
    p.product_id,
    p.title,

    -- Category information (sub-dimension)
    c.category_name,
    c.category_slug,
    pc.category_name AS parent_category,

    -- Brand information (sub-dimension)
    b.brand_name,
    b.brand_country,

    -- Sales metrics
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue,
    SUM(f.discount_amount) AS total_discounts,
    ROUND(AVG(f.unit_price), 2) AS avg_selling_price

FROM snow_fact_orders f
JOIN snow_dim_products p ON f.product_id = p.product_id
LEFT JOIN snow_dim_categories c ON p.category_id = c.category_id
LEFT JOIN snow_dim_categories pc ON c.parent_category_id = pc.category_id
LEFT JOIN snow_dim_brands b ON p.brand_id = b.brand_id

GROUP BY
    p.product_id, p.title,
    c.category_name, c.category_slug, pc.category_name,
    b.brand_name, b.brand_country

ORDER BY total_revenue DESC
LIMIT 50;


-- ============================================================================
-- 2. TOP USERS WITH LOCATION HIERARCHY
-- ============================================================================
-- Navigate through city -> state -> region

SELECT
    u.user_id,
    u.username,
    u.full_name,
    u.email,
    u.age,

    -- Role information (sub-dimension)
    r.role_name,
    r.role_description,

    -- Location hierarchy (sub-dimensions)
    ci.city_name,
    ci.population AS city_population,
    s.state_name,
    s.state_code,
    s.region,
    s.country,

    -- Purchase metrics
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.quantity) AS total_items,
    SUM(f.total_amount) AS lifetime_value,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM snow_fact_orders f
JOIN snow_dim_users u ON f.user_id = u.user_id
LEFT JOIN snow_dim_user_roles r ON u.role_id = r.role_id
LEFT JOIN snow_dim_cities ci ON u.city_id = ci.city_id
LEFT JOIN snow_dim_states s ON ci.state_id = s.state_id

GROUP BY
    u.user_id, u.username, u.full_name, u.email, u.age,
    r.role_name, r.role_description,
    ci.city_name, ci.population,
    s.state_name, s.state_code, s.region, s.country

ORDER BY lifetime_value DESC
LIMIT 100;


-- ============================================================================
-- 3. REVENUE BY REGION (GEOGRAPHIC HIERARCHY)
-- ============================================================================
-- Aggregate at different geographic levels

-- By Region
SELECT
    s.region,
    s.country,

    COUNT(DISTINCT s.state_id) AS states_in_region,
    COUNT(DISTINCT ci.city_id) AS cities_in_region,
    COUNT(DISTINCT u.user_id) AS customers_in_region,

    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS regional_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM snow_fact_orders f
JOIN snow_dim_users u ON f.user_id = u.user_id
LEFT JOIN snow_dim_cities ci ON u.city_id = ci.city_id
LEFT JOIN snow_dim_states s ON ci.state_id = s.state_id

GROUP BY s.region, s.country

ORDER BY regional_revenue DESC;


-- By State
SELECT
    s.state_name,
    s.state_code,
    s.region,

    COUNT(DISTINCT ci.city_id) AS cities_in_state,
    COUNT(DISTINCT u.user_id) AS customers_in_state,

    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS state_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM snow_fact_orders f
JOIN snow_dim_users u ON f.user_id = u.user_id
LEFT JOIN snow_dim_cities ci ON u.city_id = ci.city_id
LEFT JOIN snow_dim_states s ON ci.state_id = s.state_id

WHERE s.state_name IS NOT NULL

GROUP BY s.state_name, s.state_code, s.region

ORDER BY state_revenue DESC
LIMIT 50;


-- By City
SELECT
    ci.city_name,
    s.state_name,
    s.state_code,
    s.region,
    ci.population,

    COUNT(DISTINCT u.user_id) AS customers_in_city,
    COUNT(DISTINCT f.order_id) AS total_orders,
    SUM(f.total_amount) AS city_revenue,
    ROUND(AVG(f.total_amount), 2) AS avg_order_value

FROM snow_fact_orders f
JOIN snow_dim_users u ON f.user_id = u.user_id
LEFT JOIN snow_dim_cities ci ON u.city_id = ci.city_id
LEFT JOIN snow_dim_states s ON ci.state_id = s.state_id

WHERE ci.city_name IS NOT NULL

GROUP BY
    ci.city_name, s.state_name, s.state_code,
    s.region, ci.population

ORDER BY city_revenue DESC
LIMIT 100;


-- ============================================================================
-- 4. CATEGORY PERFORMANCE WITH HIERARCHY
-- ============================================================================
-- Analyze categories and their relationships

SELECT
    c.category_name,
    c.category_slug,
    pc.category_name AS parent_category,

    -- Product statistics
    COUNT(DISTINCT p.product_id) AS products_count,

    -- Sales metrics
    COUNT(DISTINCT f.order_id) AS orders_count,
    SUM(f.quantity) AS total_quantity_sold,
    SUM(f.total_amount) AS total_revenue,
    ROUND(AVG(f.unit_price), 2) AS avg_product_price,

    -- Market share
    ROUND(
        SUM(f.total_amount) * 100.0 / SUM(SUM(f.total_amount)) OVER (),
        2
    ) AS market_share_percent,

    -- Top brand in category
    (
        SELECT b2.brand_name
        FROM snow_fact_orders f2
        JOIN snow_dim_products p2 ON f2.product_id = p2.product_id
        JOIN snow_dim_brands b2 ON p2.brand_id = b2.brand_id
        WHERE p2.category_id = c.category_id
        GROUP BY b2.brand_name
        ORDER BY SUM(f2.total_amount) DESC
        LIMIT 1
    ) AS top_brand

FROM snow_fact_orders f
JOIN snow_dim_products p ON f.product_id = p.product_id
JOIN snow_dim_categories c ON p.category_id = c.category_id
LEFT JOIN snow_dim_categories pc ON c.parent_category_id = pc.category_id

GROUP BY
    c.category_id, c.category_name, c.category_slug,
    pc.category_name

ORDER BY total_revenue DESC;