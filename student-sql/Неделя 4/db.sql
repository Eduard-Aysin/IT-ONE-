-- Таблица продаж
CREATE TABLE sales (
    id SERIAL PRIMARY KEY,
    order_id INTEGER,
    customer_id VARCHAR(50),
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    quantity INTEGER,
    unit_price DECIMAL(10,2),
    total_price DECIMAL(10,2),
    order_date DATE,
    category VARCHAR(100),
    month VARCHAR(7)
);

-- Таблица клиентов
CREATE TABLE customers (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE,
    customer_name VARCHAR(255),
    email VARCHAR(255),
    registration_date DATE,
    region VARCHAR(100),
    customer_days INTEGER
);

-- Сводная таблица продаж
CREATE TABLE sales_summary (
    id SERIAL PRIMARY KEY,
    category VARCHAR(100),
    total_sales DECIMAL(15,2),
    total_quantity INTEGER,
    average_order_value DECIMAL(10,2),
    period_date DATE
);

-- Рейтинг товаров
CREATE TABLE product_ranking (
    id SERIAL PRIMARY KEY,
    product_id VARCHAR(50),
    product_name VARCHAR(255),
    total_sold INTEGER,
    total_revenue DECIMAL(15,2),
    rank_position INTEGER
);