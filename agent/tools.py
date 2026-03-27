from .db import TABLE, execute_query


# ── Tool functions ──────────────────────────────────────────────────────────

def get_daily_overview(date_str: str) -> dict:
    """특정 날짜 매출 개요 + 전주 대비 변화"""
    rows = execute_query(f"""
        WITH today AS (
            SELECT SUM(amount) AS revenue, COUNT(*) AS orders, COUNT(DISTINCT user_id) AS users
            FROM {TABLE} WHERE order_date = '{date_str}'
        ),
        prev AS (
            SELECT SUM(amount) AS revenue, COUNT(*) AS orders
            FROM {TABLE} WHERE order_date = DATEADD(day, -7, '{date_str}')
        )
        SELECT
            ROUND(today.revenue, 2)  AS revenue,
            today.orders,
            today.users,
            ROUND(prev.revenue, 2)   AS prev_revenue,
            prev.orders              AS prev_orders,
            ROUND((today.revenue - prev.revenue) / NULLIF(prev.revenue, 0) * 100, 1) AS revenue_change_pct,
            ROUND((today.orders  - prev.orders)  / NULLIF(prev.orders,  0) * 100, 1) AS orders_change_pct
        FROM today, prev
    """)
    return rows[0] if rows else {"error": "No data found"}


def get_channel_performance(start_date: str, end_date: str) -> list[dict]:
    """채널별 매출, 광고비, ROAS"""
    return execute_query(f"""
        SELECT
            ad_channel,
            COUNT(*)                                              AS orders,
            ROUND(SUM(amount), 2)                                 AS revenue,
            ROUND(SUM(ad_spend), 2)                               AS ad_spend,
            ROUND(SUM(amount) / NULLIF(SUM(ad_spend), 0), 2)     AS roas,
            ROUND(AVG(amount), 2)                                 AS avg_order_value
        FROM {TABLE}
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY ad_channel
        ORDER BY revenue DESC
    """)


def get_category_breakdown(start_date: str, end_date: str) -> list[dict]:
    """카테고리별 매출 비중 포함 분해"""
    return execute_query(f"""
        SELECT
            category,
            COUNT(*)                                                          AS orders,
            ROUND(SUM(amount), 2)                                             AS revenue,
            ROUND(AVG(amount), 2)                                             AS avg_order_value,
            ROUND(SUM(amount) / SUM(SUM(amount)) OVER () * 100, 1)           AS revenue_share_pct
        FROM {TABLE}
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY category
        ORDER BY revenue DESC
    """)


def get_daily_trend(metric: str, days: int = 14) -> list[dict]:
    """최근 N일 일별 트렌드. metric: revenue | orders | users | avg_order_value | ad_spend"""
    metric_sql = {
        "revenue":         "ROUND(SUM(amount), 2)",
        "orders":          "COUNT(*)",
        "users":           "COUNT(DISTINCT user_id)",
        "avg_order_value": "ROUND(AVG(amount), 2)",
        "ad_spend":        "ROUND(SUM(ad_spend), 2)",
    }
    col = metric_sql.get(metric, "ROUND(SUM(amount), 2)")
    return execute_query(f"""
        SELECT order_date, {col} AS value
        FROM {TABLE}
        WHERE order_date >= DATEADD(day, -{days}, CURRENT_DATE)
        GROUP BY order_date
        ORDER BY order_date ASC
    """)


def get_promo_performance(start_date: str, end_date: str) -> list[dict]:
    """프로모 코드별 주문 수, 매출, 평균 주문 금액"""
    return execute_query(f"""
        SELECT
            COALESCE(NULLIF(promo_code, ''), 'NO_PROMO') AS promo,
            COUNT(*)                                      AS orders,
            ROUND(SUM(amount), 2)                         AS revenue,
            ROUND(AVG(amount), 2)                         AS avg_order_value
        FROM {TABLE}
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY promo
        ORDER BY revenue DESC
    """)


def get_region_breakdown(start_date: str, end_date: str) -> list[dict]:
    """지역별 매출"""
    return execute_query(f"""
        SELECT
            region,
            COUNT(*)             AS orders,
            ROUND(SUM(amount), 2) AS revenue,
            ROUND(AVG(amount), 2) AS avg_order_value
        FROM {TABLE}
        WHERE order_date BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY region
        ORDER BY revenue DESC
    """)


def compare_two_periods(
    period1_start: str, period1_end: str,
    period2_start: str, period2_end: str,
) -> dict:
    """두 기간의 채널별·카테고리별 매출 비교 및 변화율 — 원인 분석에 사용"""
    by_channel = execute_query(f"""
        SELECT
            ad_channel,
            ROUND(SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END), 2) AS period1_revenue,
            ROUND(SUM(CASE WHEN order_date BETWEEN '{period2_start}' AND '{period2_end}' THEN amount END), 2) AS period2_revenue,
            ROUND(
                (SUM(CASE WHEN order_date BETWEEN '{period2_start}' AND '{period2_end}' THEN amount END)
                 - SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END))
                / NULLIF(SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END), 0) * 100
            , 1) AS change_pct
        FROM {TABLE}
        WHERE order_date BETWEEN '{period1_start}' AND '{period2_end}'
        GROUP BY ad_channel
        ORDER BY period2_revenue DESC NULLS LAST
    """)
    by_category = execute_query(f"""
        SELECT
            category,
            ROUND(SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END), 2) AS period1_revenue,
            ROUND(SUM(CASE WHEN order_date BETWEEN '{period2_start}' AND '{period2_end}' THEN amount END), 2) AS period2_revenue,
            ROUND(
                (SUM(CASE WHEN order_date BETWEEN '{period2_start}' AND '{period2_end}' THEN amount END)
                 - SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END))
                / NULLIF(SUM(CASE WHEN order_date BETWEEN '{period1_start}' AND '{period1_end}' THEN amount END), 0) * 100
            , 1) AS change_pct
        FROM {TABLE}
        WHERE order_date BETWEEN '{period1_start}' AND '{period2_end}'
        GROUP BY category
        ORDER BY period2_revenue DESC NULLS LAST
    """)
    return {
        "period1": f"{period1_start} ~ {period1_end}",
        "period2": f"{period2_start} ~ {period2_end}",
        "by_channel": by_channel,
        "by_category": by_category,
    }


# ── OpenAI tool schemas ─────────────────────────────────────────────────────

TOOL_SCHEMAS = [
    {
        "type": "function",
        "function": {
            "name": "get_daily_overview",
            "description": "특정 날짜의 총 매출, 주문 수, 사용자 수를 조회하고 전주 동일 날짜와 비교합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "date_str": {"type": "string", "description": "조회할 날짜 (YYYY-MM-DD)"}
                },
                "required": ["date_str"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_channel_performance",
            "description": "광고 채널별 매출, 광고비, ROAS, 평균 주문 금액을 조회합니다. 어떤 채널이 효율적인지 분석할 때 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "시작 날짜 (YYYY-MM-DD)"},
                    "end_date":   {"type": "string", "description": "종료 날짜 (YYYY-MM-DD)"}
                },
                "required": ["start_date", "end_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_category_breakdown",
            "description": "제품 카테고리별 매출, 주문 수, 매출 비중을 조회합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "시작 날짜 (YYYY-MM-DD)"},
                    "end_date":   {"type": "string", "description": "종료 날짜 (YYYY-MM-DD)"}
                },
                "required": ["start_date", "end_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_daily_trend",
            "description": "최근 N일간 특정 지표의 일별 추이를 조회합니다. 트렌드나 이상값을 확인할 때 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "metric": {
                        "type": "string",
                        "enum": ["revenue", "orders", "users", "avg_order_value", "ad_spend"],
                        "description": "조회할 지표"
                    },
                    "days": {"type": "integer", "description": "조회할 일수 (기본 14)", "default": 14}
                },
                "required": ["metric"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_promo_performance",
            "description": "프로모션 코드별 주문 수, 매출, 평균 주문 금액을 조회합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "시작 날짜 (YYYY-MM-DD)"},
                    "end_date":   {"type": "string", "description": "종료 날짜 (YYYY-MM-DD)"}
                },
                "required": ["start_date", "end_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "get_region_breakdown",
            "description": "지역별 매출, 주문 수, 평균 주문 금액을 조회합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_date": {"type": "string", "description": "시작 날짜 (YYYY-MM-DD)"},
                    "end_date":   {"type": "string", "description": "종료 날짜 (YYYY-MM-DD)"}
                },
                "required": ["start_date", "end_date"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "compare_two_periods",
            "description": "두 기간의 채널별·카테고리별 매출을 비교하고 변화율을 계산합니다. '왜 매출이 떨어졌나?' 같은 원인 분석 질문에 사용합니다.",
            "parameters": {
                "type": "object",
                "properties": {
                    "period1_start": {"type": "string", "description": "비교 기준 기간 시작 (YYYY-MM-DD)"},
                    "period1_end":   {"type": "string", "description": "비교 기준 기간 종료 (YYYY-MM-DD)"},
                    "period2_start": {"type": "string", "description": "비교 대상 기간 시작 (YYYY-MM-DD)"},
                    "period2_end":   {"type": "string", "description": "비교 대상 기간 종료 (YYYY-MM-DD)"}
                },
                "required": ["period1_start", "period1_end", "period2_start", "period2_end"]
            }
        }
    },
]

TOOL_MAP = {
    "get_daily_overview":     get_daily_overview,
    "get_channel_performance": get_channel_performance,
    "get_category_breakdown": get_category_breakdown,
    "get_daily_trend":        get_daily_trend,
    "get_promo_performance":  get_promo_performance,
    "get_region_breakdown":   get_region_breakdown,
    "compare_two_periods":    compare_two_periods,
}
