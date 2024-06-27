-- Check if the indexes exist and create them if they do not
SET @create_index_sql := (SELECT IF(
    (SELECT COUNT(*)
     FROM INFORMATION_SCHEMA.STATISTICS
     WHERE table_schema = 'opt_db' AND table_name = 'opt_orders' AND index_name = 'idx_opt_orders_client_id') = 0,
    'CREATE INDEX idx_opt_orders_client_id ON opt_orders(client_id)',
    'SELECT 1'));
PREPARE stmt FROM @create_index_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

SET @create_index_sql := (SELECT IF(
    (SELECT COUNT(*)
     FROM INFORMATION_SCHEMA.STATISTICS
     WHERE table_schema = 'opt_db' AND table_name = 'opt_orders' AND index_name = 'idx_opt_orders_product_id') = 0,
    'CREATE INDEX idx_opt_orders_product_id ON opt_orders(product_id)',
    'SELECT 1'));
PREPARE stmt FROM @create_index_sql;
EXECUTE stmt;
DEALLOCATE PREPARE stmt;

-- Non-optimized query example with EXPLAIN
EXPLAIN SELECT o.order_id, o.order_date, c.name AS client_name, p.product_name
FROM opt_orders o
JOIN opt_clients c ON o.client_id = c.id
JOIN opt_products p ON o.product_id = p.product_id
WHERE o.order_date > '2023-01-01';

-- Optimized query example using CTEs and indexes with EXPLAIN
EXPLAIN WITH cte_orders AS (
    SELECT order_id, order_date, client_id, product_id
    FROM opt_orders
    WHERE order_date > '2023-01-01'
),
cte_clients AS (
    SELECT id, name
    FROM opt_clients
),
cte_products AS (
    SELECT product_id, product_name
    FROM opt_products
)
SELECT o.order_id, o.order_date, c.name AS client_name, p.product_name
FROM cte_orders o
JOIN cte_clients c ON o.client_id = c.id
JOIN cte_products p ON o.product_id = p.product_id;
