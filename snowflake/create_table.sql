USE DATABASE SNOWFLAKE_LEARNING_DB;

CREATE TABLE IF NOT EXISTS ecommerce_orders (
    order_id      VARCHAR(50),
    order_date    DATE,
    user_id       VARCHAR(20),
    region        VARCHAR(50),
    product_id    VARCHAR(20),
    category      VARCHAR(50),
    amount        FLOAT,
    promo_code    VARCHAR(20),
    ad_channel    VARCHAR(20),
    ad_spend      FLOAT,
    loaded_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);