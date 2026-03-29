from datetime import datetime, timedelta
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE = "snowflake_learning_db.public.ecommerce_orders"


def generate_forecast(date_str: str, days_ahead: int = 7) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    rows = hook.get_records(f"""
        SELECT order_date, SUM(amount) AS revenue
        FROM {TABLE}
        WHERE order_date BETWEEN DATEADD(day, -28, '{date_str}') AND '{date_str}'
        GROUP BY order_date ORDER BY order_date
    """)

    if len(rows) < 7:
        return {"as_of": date_str, "forecast": [], "note": "Insufficient data"}

    revenues = [float(r[1]) for r in rows]
    dates    = [r[0] if isinstance(r[0], str) else r[0].strftime("%Y-%m-%d") for r in rows]

    # 요일별 평균 매출 (seasonality factor)
    day_revenues: dict[int, list] = {}
    for d, rev in zip(dates, revenues):
        weekday = datetime.strptime(d, "%Y-%m-%d").weekday()
        day_revenues.setdefault(weekday, []).append(rev)

    overall_avg = sum(revenues) / len(revenues)
    day_avg = {wd: sum(v) / len(v) for wd, v in day_revenues.items()}

    # 최근 7일 선형 트렌드
    recent = revenues[-7:]
    trend = (recent[-1] - recent[0]) / 6

    base_date = datetime.strptime(date_str, "%Y-%m-%d")
    forecast = []
    for i in range(1, days_ahead + 1):
        future = base_date + timedelta(days=i)
        wd = future.weekday()
        seasonal_ratio = day_avg.get(wd, overall_avg) / overall_avg if overall_avg > 0 else 1
        predicted = max((overall_avg + trend * i) * seasonal_ratio, 0)
        forecast.append({
            "date": future.strftime("%Y-%m-%d"),
            "day":  future.strftime("%a"),
            "predicted_revenue": round(predicted, 2),
        })

    trend_dir = "up" if trend > 50 else "down" if trend < -50 else "flat"

    print(f"[{date_str}] Forecast generated | trend={trend_dir} | next7d avg=${sum(f['predicted_revenue'] for f in forecast)/7:,.0f}")
    return {
        "as_of":    date_str,
        "forecast": forecast,
        "avg_28d":  round(overall_avg, 2),
        "trend":    trend_dir,
    }
