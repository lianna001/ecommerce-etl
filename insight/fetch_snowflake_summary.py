from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


def fetch_summary(date_str: str) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    # 어제 vs 7일 전 매출 비교
    result = hook.get_first(f"""
        WITH today AS (
            SELECT 
                SUM(amount)            AS revenue,
                COUNT(*)               AS orders,
                COUNT(DISTINCT user_id) AS users
            FROM snowflake_learning_db.public.ecommerce_orders
            WHERE order_date = '{date_str}'
        ),
        week_ago AS (
            SELECT 
                SUM(amount) AS revenue,
                COUNT(*)    AS orders
            FROM snowflake_learning_db.public.ecommerce_orders
            WHERE order_date = DATEADD(day, -7, '{date_str}')
        )
        SELECT 
            today.revenue, today.orders, today.users,
            week_ago.revenue AS prev_revenue,
            week_ago.orders  AS prev_orders
        FROM today, week_ago
    """)

    # 카테고리별 매출
    categories = hook.get_records(f"""
        SELECT category, SUM(amount) AS revenue, COUNT(*) AS orders
        FROM snowflake_learning_db.public.ecommerce_orders
        WHERE order_date = '{date_str}'
        GROUP BY category
        ORDER BY revenue DESC
    """)

    # 채널별 ROAS
    channels = hook.get_records(f"""
        SELECT 
            ad_channel,
            SUM(amount)                                        AS revenue,
            SUM(ad_spend)                                      AS spend,
            ROUND(SUM(amount) / NULLIF(SUM(ad_spend), 0), 2)  AS roas
        FROM snowflake_learning_db.public.ecommerce_orders
        WHERE order_date = '{date_str}'
        GROUP BY ad_channel
        ORDER BY roas DESC
    """)

    # 프로모션 효과
    promos = hook.get_records(f"""
        SELECT 
            COALESCE(NULLIF(promo_code, ''), 'NO_PROMO') AS promo,
            COUNT(*)                                      AS orders,
            ROUND(AVG(amount), 2)                         AS avg_amount
        FROM snowflake_learning_db.public.ecommerce_orders
        WHERE order_date = '{date_str}'
        GROUP BY promo
        ORDER BY orders DESC
    """)

    summary = {
        "date": date_str,
        "overview": {
            "revenue":            round(result[0], 2),
            "orders":             result[1],
            "users":              result[2],
            "prev_revenue":       round(result[3], 2),
            "prev_orders":        result[4],
            "revenue_change_pct": round((result[0] - result[3]) / result[3] * 100, 1)
        },
        "categories": [
            {"category": r[0], "revenue": round(r[1], 2), "orders": r[2]}
            for r in categories
        ],
        "channels": [
            {"channel": r[0], "revenue": round(r[1], 2), "spend": round(r[2], 2), "roas": r[3]}
            for r in channels
        ],
        "promos": [
            {"promo": r[0], "orders": r[1], "avg_amount": r[2]}
            for r in promos
        ]
    }

    print(f"[{date_str}] Snowflake 집계 완료 | 매출 ${summary['overview']['revenue']}")
    return summary