import sys
sys.path.append("/opt/airflow/ecommerce-etl")

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pandas as pd
import pytz

from insight.check_data_quality import check_data_quality


def load_to_snowflake(**kwargs):
    CSV_PATH = "/opt/airflow/ecommerce-etl/data/sample/orders.csv"

    df = pd.read_csv(CSV_PATH)

    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    hook.run("DELETE FROM snowflake_learning_db.public.ecommerce_orders")

    engine = hook.get_sqlalchemy_engine()
    df.to_sql(name="ecommerce_orders", con=engine, schema="PUBLIC", if_exists="append", index=False)
    print(f"전체 {len(df)}건 Snowflake 적재 완료")


with DAG(
    dag_id="ecommerce_daily_data",
    start_date=datetime(2026, 3, 1),
    schedule="@daily",
    catchup=False,
    default_args={"owner": "lianna", "retries": 1},
) as dag:

    generate_data = BashOperator(
        task_id="generate_daily_data",
        bash_command=(
    "TZ=America/Los_Angeles "
    "OUTPUT_PATH=/opt/airflow/ecommerce-etl/data/sample/orders.csv "
    "python3 /opt/airflow/ecommerce-etl/extract/generate_daily_data.py "
    "{{ ds }}"
)
    )

    load_data = PythonOperator(
        task_id="load_to_snowflake",
        python_callable=load_to_snowflake,
    )

    quality_check = PythonOperator(
        task_id="check_data_quality",
        python_callable=lambda **kwargs: check_data_quality(kwargs["ds"]),
    )

    generate_data >> load_data >> quality_check