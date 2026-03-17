# E-Commerce ETL Pipeline & ads/promotion impact analysis

## Business Question
> What is the impact of ad/promotions on sales revenue?

---

## Project Overview

### Extract & Transform
- Generates daily e-commerce order data (orders, users, products, ad spend) using Python
- Applies realistic patterns: weekend spikes, end-of-month promotion boosts, promo code discounts
- Data is cleaned at generation time with standardized formats and reproducible random seeds
- Historical data backfilled from **2026-03-01 to 2026-03-15** using `backfill.py`

### Load
- Daily data loaded into **Snowflake** (`ecommerce_orders` table) via **Airflow DAG**
- Idempotent logic: existing records for the same date are deleted before each load
- Pipeline runs automatically on `@daily` schedule

### Visualize
- Snowflake connected to **Tableau** for real-time dashboard
- Dashboard details coming soon

---

## Tech Stack
`Python` `Pandas` `Apache Airflow` `Snowflake` `Tableau` `Docker` `GitHub`
