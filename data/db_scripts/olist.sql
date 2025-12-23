/*
 * Olist Database Schema
 * Description: DDL for Olist E-commerce dataset.
 */

CREATE DATABASE IF NOT EXISTS olist;
USE olist;

CREATE TABLE IF NOT EXISTS product_category_name_translation (
    product_category_name VARCHAR(64),
    product_category_name_english VARCHAR(64),
    PRIMARY KEY (product_category_name)
);

CREATE TABLE IF NOT EXISTS geolocation (
    geolocation_zip_code_prefix INT,
    geolocation_lat FLOAT,
    geolocation_lng FLOAT,
    geolocation_city VARCHAR(64),
    geolocation_state VARCHAR(64)
);

CREATE TABLE IF NOT EXISTS sellers (
    seller_id VARCHAR(64),
    seller_zip_code_prefix INT,
    seller_city VARCHAR(64),
    seller_state VARCHAR(64),
    PRIMARY KEY (seller_id)
);

CREATE TABLE IF NOT EXISTS customers (
    customer_id VARCHAR(64),
    customer_unique_id VARCHAR(32),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(64),
    customer_state VARCHAR(64),
    PRIMARY KEY (customer_id)
);

CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(64),
    product_category_name VARCHAR(64),
    product_name_length INT,
    product_description_length FLOAT,
    product_photos_qty INT,
    product_weight_g INT,
    product_length_cm FLOAT,
    product_height_cm FLOAT,
    product_width_cm FLOAT,
    PRIMARY KEY (product_id),
    FOREIGN KEY (product_category_name) REFERENCES product_category_name_translation(product_category_name)
);

CREATE TABLE IF NOT EXISTS orders (
    order_id VARCHAR(64),
    customer_id VARCHAR(64),
    order_status VARCHAR(32),
    order_purchase_timestamp DATE,
    order_approved_at DATE,
    order_delivered_carrier_date DATE,
    order_delivered_customer_date DATE,
    order_estimated_delivery_date DATE,
    PRIMARY KEY (order_id),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

CREATE TABLE IF NOT EXISTS order_items (
    order_id VARCHAR(64),
    order_item_id INT,
    product_id VARCHAR(64),
    seller_id VARCHAR(64),
    shipping_limit_date DATE,
    price FLOAT,
    freight_value FLOAT,
    FOREIGN KEY (product_id) REFERENCES products(product_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (seller_id) REFERENCES sellers(seller_id)
);

CREATE TABLE IF NOT EXISTS payments (
    order_id VARCHAR(64),
    payment_sequential INT,
    payment_type VARCHAR(32),
    payment_installments FLOAT,
    payment_value FLOAT,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);

CREATE TABLE IF NOT EXISTS order_reviews (
    review_id VARCHAR(64),
    order_id VARCHAR(64),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATE,
    review_answer_timestamp DATE,
    FOREIGN KEY (order_id) REFERENCES orders(order_id)
);
