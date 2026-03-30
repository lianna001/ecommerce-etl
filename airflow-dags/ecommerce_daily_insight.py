import sys
sys.path.append("/opt/airflow/ecommerce-etl")

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from insight.fetch_snowflake_summary import fetch_summary
from insight.generate_insight import generate_insight
from insight.detect_anomaly import detect_anomaly
from insight.fetch_news import fetch_and_summarize_news
from insight.send_email import send_daily_report
from insight.update_notion import update_notion_page


def run_fetch_summary(**kwargs):
    summary = fetch_summary(kwargs["ds"])
    kwargs["ti"].xcom_push(key="summary", value=summary)


def run_generate_insight(**kwargs):
    summary = kwargs["ti"].xcom_pull(key="summary", task_ids="fetch_snowflake_summary")
    insight = generate_insight(summary)
    kwargs["ti"].xcom_push(key="insight", value=insight)


def run_detect_anomaly(**kwargs):
    anomaly = detect_anomaly(kwargs["ds"])
    kwargs["ti"].xcom_push(key="anomaly", value=anomaly)


def run_fetch_news(**kwargs):
    news = fetch_and_summarize_news()
    kwargs["ti"].xcom_push(key="news", value=news)


def run_send_email(**kwargs):
    ti       = kwargs["ti"]
    date_str = kwargs["ds"]
    insight  = ti.xcom_pull(key="insight", task_ids="generate_insight")
    anomaly  = ti.xcom_pull(key="anomaly", task_ids="detect_anomaly")
    news     = ti.xcom_pull(key="news",    task_ids="fetch_news")
    send_daily_report(date_str=date_str, insight=insight, news=news, anomaly=anomaly)


def run_update_notion(**kwargs):
    ti       = kwargs["ti"]
    date_str = kwargs["ds"]
    insight  = ti.xcom_pull(key="insight", task_ids="generate_insight")
    anomaly  = ti.xcom_pull(key="anomaly", task_ids="detect_anomaly")
    news     = ti.xcom_pull(key="news",    task_ids="fetch_news")
    update_notion_page(date_str=date_str, insight=insight, news=news, anomaly=anomaly)


with DAG(
    dag_id="ecommerce_daily_insight",
    start_date=datetime(2026, 3, 1),
    schedule="0 9 * * *",
    catchup=False,
    default_args={"owner": "lianna", "retries": 1},
) as dag:

    t1 = PythonOperator(task_id="fetch_snowflake_summary", python_callable=run_fetch_summary)
    t2 = PythonOperator(task_id="generate_insight",        python_callable=run_generate_insight)
    t3 = PythonOperator(task_id="detect_anomaly",          python_callable=run_detect_anomaly)
    t4 = PythonOperator(task_id="fetch_news",              python_callable=run_fetch_news)
    t5 = PythonOperator(task_id="send_email",              python_callable=run_send_email)
    t6 = PythonOperator(task_id="update_notion",           python_callable=run_update_notion)

    t1 >> [t2, t3, t4] >> t5 >> t6
