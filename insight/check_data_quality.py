from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE = "snowflake_learning_db.public.ecommerce_orders"


def check_data_quality(date_str: str) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    issues = []

    # 1. 행 수 확인
    row_count = hook.get_first(f"SELECT COUNT(*) FROM {TABLE} WHERE order_date = '{date_str}'"  )[0]
    if row_count == 0:
        issues.append({"type": "CRITICAL", "msg": f"No data loaded for {date_str}"})
    elif row_count < 100:
        issues.append({"type": "WARNING", "msg": f"Low row count: {row_count} (expected ~200)"})

    # 2. null 확인
    nulls = hook.get_first(f"""
        SELECT
            SUM(CASE WHEN order_id  IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN amount    IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN ad_channel IS NULL THEN 1 ELSE 0 END),
            SUM(CASE WHEN category  IS NULL THEN 1 ELSE 0 END)
        FROM {TABLE} WHERE order_date = '{date_str}'
    """)
    for count, field in zip(nulls, ["order_id", "amount", "ad_channel", "category"]):
        if count > 0:
            issues.append({"type": "WARNING", "msg": f"Null values in {field}: {count}"})

    # 3. 중복 order_id 확인
    dups = hook.get_first(f"""
        SELECT COUNT(*) - COUNT(DISTINCT order_id)
        FROM {TABLE} WHERE order_date = '{date_str}'
    """)[0]
    if dups > 0:
        issues.append({"type": "WARNING", "msg": f"Duplicate order_ids: {dups}"})

    # 4. 비정상 금액 확인
    neg = hook.get_first(f"SELECT COUNT(*) FROM {TABLE} WHERE order_date = '{date_str}' AND amount <= 0")[0]
    if neg > 0:
        issues.append({"type": "WARNING", "msg": f"Invalid amount (<=0): {neg}"})

    result = {
        "date": date_str,
        "row_count": row_count,
        "issues": issues,
        "passed": all(i["type"] != "CRITICAL" for i in issues),
    }

    if not result["passed"]:
        raise ValueError(f"[Data Quality CRITICAL] {date_str}: {issues}")

    print(f"[{date_str}] Data quality OK | {row_count} rows | {len(issues)} warnings")
    return result
