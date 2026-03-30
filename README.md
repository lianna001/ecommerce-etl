# E-Commerce ETL Pipeline & AI-Powered Analytics

## Business Question
> Which ad channels and promotional offers drive the most revenue — and how does that vary by product category?

**[📊 Tableau Dashboard](https://public.tableau.com/app/profile/lianna.lee/viz/ecommerce_dashboard_v2/Dashboard12)** | **[🤖 AI Agent (Streamlit)](https://lianna-ecommerce-etl.streamlit.app/)** | **[📁 Notion Portfolio](https://www.notion.so/E-Commerce-Data-Engineer-Analyst-Project-33208d1af1618082a085e37f9c9980a0)**

---

## Project Goals

### Goal 1 — End-to-End ETL Pipeline & Visualization
Synthetic e-commerce data generated daily with realistic seasonality patterns, loaded into Snowflake via Airflow, and visualized in Tableau with WoW and WTD metrics.

### Goal 2 — AI-Automated Daily Insight Pipeline
Airflow DAG running daily at 9AM that fetches Snowflake KPIs, generates a GPT-4o-mini business insight report, detects weekly revenue anomalies (triggers alert email if WoW change exceeds ±25%), scrapes industry news, and delivers everything as an HTML email.

### Goal 3 — AI Agent Integration within Tableau
GPT-4o-powered chatbot with 7 pre-built Snowflake query tools. Uses OpenAI function calling to autonomously select and chain tools based on user questions — deployed on Streamlit and embedded inside the Tableau dashboard.

---

## Tech Stack
`Python` `SQL` `Apache Airflow` `Snowflake` `Tableau` `Docker` `OpenAI API` `Streamlit` `GitHub`
