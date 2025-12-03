-- ============================================================================
-- SQL QUERIES FOR 3NF SCHEMA
-- ============================================================================
-- These queries demonstrate working with normalized data using JOINs
-- ============================================================================

-- ============================================================================
-- 1. LIST OF ORDERS WITH FULL DETAILS (JOIN 5 TABLES)
-- ============================================================================
-- Show order details with user info, products, and location

SELECT
    o.id AS order_id,
    o.order_date,
    o.status,
    o.total,
    o.discounted_total,

    -- User information
    u.username,
    u.email,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,

    -- User address
    a.city,
    a.state,
    a.postal_code,

    -- Order items
    oi.quantity,
    oi.price AS unit_price,
    oi.total AS item_total,

    -- Product information
    p.title AS product_name,
    p.brand,
    c.name AS category

FROM orders o
JOIN users u ON o.user_id = u.id
LEFT JOIN addresses a ON u.address_id = a.id
JOIN order_items oi ON o.id = oi.order_id
JOIN products p ON oi.product_id = p.id
LEFT JOIN categories c ON p.category_id = c.id

ORDER BY o.order_date DESC, o.id
LIMIT 100;


-- ============================================================================
-- 2. REVENUE BY USER
-- ============================================================================
-- Calculate total revenue per user with order statistics

SELECT
    u.id AS user_id,
    u.username,
    CONCAT(u.first_name, ' ', u.last_name) AS full_name,
    u.email,

    -- Address
    a.city,
    a.state,

    -- Statistics
    COUNT(DISTINCT o.id) AS total_orders,
    COUNT(oi.id) AS total_items,
    SUM(oi.quantity) AS total_quantity,

    -- Revenue
    SUM(oi.total) AS total_revenue,
    SUM(o.discounted_total) AS total_discounted_revenue,
    ROUND(AVG(o.total), 2) AS avg_order_value,

    -- First and last order
    MIN(o.order_date) AS first_order_date,
    MAX(o.order_date) AS last_order_date

FROM users u
LEFT JOIN addresses a ON u.address_id = a.id
JOIN orders o ON u.id = o.user_id
JOIN order_items oi ON o.id = oi.order_id

GROUP BY
    u.id, u.username, u.first_name, u.last_name,
    u.email, a.city, a.state

ORDER BY total_revenue DESC
LIMIT 50;


-- ============================================================================
-- 3. ORDERS BY DATE RANGE
-- ============================================================================
-- Filter orders within a specific date range with details

SELECT
    DATE(o.order_date) AS order_date,
    COUNT(DISTINCT o.id) AS orders_count,
    SUM(o.total_quantity) AS total_items_sold,
    SUM(o.total) AS daily_revenue,
    SUM(o.discounted_total) AS daily_discounted_revenue,
    ROUND(AVG(o.total), 2) AS avg_order_value,

    -- Top product category for the day
    (
        SELECT c.name
        FROM order_items oi2
        JOIN products p2 ON oi2.product_id = p2.id
        JOIN categories c ON p2.category_id = c.id
        WHERE oi2.order_id IN (
            SELECT id FROM orders WHERE DATE(order_date) = DATE(o.order_date)
        )
        GROUP BY c.name
        ORDER BY COUNT(*) DESC
        LIMIT 1
    ) AS top_category

FROM orders o

WHERE o.order_date BETWEEN '2024-01-01' AND '2024-12-31'

GROUP BY DATE(o.order_date)
ORDER BY order_date DESC;
