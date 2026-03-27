import os
import snowflake.connector
from contextlib import contextmanager

TABLE = "snowflake_learning_db.public.ecommerce_orders"


@contextmanager
def get_connection():
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        database=os.environ.get("SNOWFLAKE_DATABASE", "SNOWFLAKE_LEARNING_DB"),
        schema=os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
    )
    try:
        yield conn
    finally:
        conn.close()


def execute_query(sql: str) -> list[dict]:
    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql)
        cols = [desc[0].lower() for desc in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
