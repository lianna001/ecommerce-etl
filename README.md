# E-Commerce ETL Pipeline & Ads/Promotion Impact Analysis

## Business Question
> Which ad channels and promotional offers drive the most revenue — and how does that vary by product category?

---

## Project Highlights
- **End-to-end ETL pipeline** — custom Python data generation with realistic e-commerce patterns, Airflow-orchestrated daily loads into Snowflake
- **Interactive [Tableau Dashboard](https://public.tableau.com/views/ecommerce_dashboard_17739708179770/Dashboard12?:language=en&:display_count=n&:origin=viz_share_link)** — tracks ad vs. organic sales, week-over-week(WoW) trends, and category-level performance

---

## Project Overview

### Extract & Transform
- Generates daily e-commerce order data (orders, users, products, ad spend) using Python
- Applies realistic patterns: weekend spikes, monthly seasonality, weekly category growth trends, and end-of-month promotion boosts
- Ad channel distribution reflects real-world mix (Meta-heavy paid, meaningful organic share)
- Data is cleaned at generation time with standardized formats and reproducible random seeds
- Historical data backfilled from 2026-03-01 using `backfill.py`

### Load
- Daily data loaded into Snowflake (`ecommerce_orders` table) via Airflow DAG running in Docker
- Same-date records are deleted before each load to prevent duplicates, ensuring backfilled data stays clean on reruns
- Pipeline runs automatically on `@daily` schedule

### Visualize
- Snowflake connected to Tableau via Custom SQL for week-to-date(WTD) WoW calculations to capture more accurate in-progress weekly trends, with remaining aggregations handled automatically
- Dashboard answers:
  - How much of total revenue comes from paid ads vs. organic?
  - Which ad channels perform best?
  - Which promotions (FREESHIP vs. DISCOUNT10) drive more sales?
  - How are sales trending week-over-week, overall and by category?
- LLM integrated for data insights interpretation (working in progress as of 3/25)

### AI Agent
- Daily e-commerce news & data insights auto updates via email/Notion (working in progress as of 3/25)

---

## Tech Stack
`Python` `SQL` `Apache Airflow` `Snowflake` `Tableau` `Docker` `GitHub` `OpenAI` `API` `Notion`
