import os
import statistics
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE = "snowflake_learning_db.public.ecommerce_orders"


def detect_anomaly(date_str: str) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    # 최근 7일 일별 매출 (오늘 제외)
    past = hook.get_records(f"""
        SELECT order_date, SUM(amount) AS revenue
        FROM {TABLE}
        WHERE order_date BETWEEN DATEADD(day, -8, '{date_str}') AND DATEADD(day, -1, '{date_str}')
        GROUP BY order_date ORDER BY order_date
    """)

    today = hook.get_first(f"""
        SELECT SUM(amount), COUNT(*) FROM {TABLE} WHERE order_date = '{date_str}'
    """)

    if not past or not today or not today[0]:
        return {"is_anomaly": False, "date": date_str}

    past_revenues = [float(r[1]) for r in past]
    today_revenue = float(today[0])
    avg = statistics.mean(past_revenues)
    stdev = statistics.stdev(past_revenues) if len(past_revenues) > 1 else 0
    z_score = (today_revenue - avg) / stdev if stdev > 0 else 0
    change_pct = (today_revenue - avg) / avg * 100 if avg > 0 else 0

    is_anomaly = abs(z_score) > 2 or abs(change_pct) > 25
    severity = "HIGH" if (abs(z_score) > 3 or abs(change_pct) > 40) else "MEDIUM" if is_anomaly else "NORMAL"

    # 채널별 변화 (이상 원인 파악)
    channels = hook.get_records(f"""
        SELECT
            ad_channel,
            SUM(CASE WHEN order_date = '{date_str}' THEN amount ELSE 0 END) AS today_rev,
            AVG(CASE WHEN order_date != '{date_str}' THEN daily_rev END)     AS avg_rev
        FROM (
            SELECT order_date, ad_channel, SUM(amount) AS daily_rev
            FROM {TABLE}
            WHERE order_date BETWEEN DATEADD(day, -8, '{date_str}') AND '{date_str}'
            GROUP BY order_date, ad_channel
        )
        GROUP BY ad_channel
        ORDER BY ABS(SUM(CASE WHEN order_date = '{date_str}' THEN amount ELSE 0 END)
                   - AVG(CASE WHEN order_date != '{date_str}' THEN daily_rev END)) DESC
    """)

    top_movers = [
        {
            "channel": r[0],
            "today": round(float(r[1] or 0), 2),
            "avg_7d": round(float(r[2] or 0), 2),
            "change_pct": round((float(r[1] or 0) - float(r[2] or 0)) / max(float(r[2] or 1), 1) * 100, 1),
        }
        for r in channels[:3]
    ]

    result = {
        "date": date_str,
        "is_anomaly": is_anomaly,
        "severity": severity,
        "today_revenue": round(today_revenue, 2),
        "avg_revenue_7d": round(avg, 2),
        "change_pct": round(change_pct, 1),
        "z_score": round(z_score, 2),
        "top_movers": top_movers,
    }

    print(f"[{date_str}] Anomaly: {'YES (' + severity + ')' if is_anomaly else 'NORMAL'} | {change_pct:.1f}% vs 7d avg")

    # 이상 감지 시 즉시 알림 이메일 발송
    if is_anomaly:
        _send_anomaly_alert(result)

    return result


def _send_anomaly_alert(anomaly: dict):
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pw   = os.environ.get("GMAIL_APP_PASSWORD")
    to_email   = os.environ.get("REPORT_TO_EMAIL", gmail_user)

    if not gmail_user or not gmail_pw:
        print("[Anomaly Alert] Email credentials missing, skipping alert.")
        return

    direction = "dropped" if anomaly["change_pct"] < 0 else "spiked"
    emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"

    movers_text = "\n".join(
        f"  • {m['channel']}: ${m['today']:,.0f} ({m['change_pct']:+.1f}% vs 7d avg)"
        for m in anomaly["top_movers"]
    )

    body = f"""
<html><body style="font-family:sans-serif;max-width:600px;margin:auto;padding:24px">
  <h2>{emoji} Revenue Anomaly Detected — {anomaly['date']}</h2>
  <p>Today's revenue <strong>{direction} {abs(anomaly['change_pct']):.1f}%</strong> vs 7-day average.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><td style="padding:8px;background:#f5f5f5"><b>Today</b></td><td style="padding:8px">${anomaly['today_revenue']:,.2f}</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>7-Day Avg</b></td><td style="padding:8px">${anomaly['avg_revenue_7d']:,.2f}</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>Change</b></td><td style="padding:8px">{anomaly['change_pct']:+.1f}%</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>Severity</b></td><td style="padding:8px">{anomaly['severity']}</td></tr>
  </table>
  <h3>Top Channel Movers</h3>
  <pre style="background:#f9f9f9;padding:12px;border-radius:4px">{movers_text}</pre>
</body></html>
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{emoji} [Anomaly Alert] {anomaly['date']} Revenue {direction} {abs(anomaly['change_pct']):.0f}%"
    msg["From"]    = gmail_user
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_pw)
        server.sendmail(gmail_user, to_email, msg.as_string())

    print(f"[Anomaly Alert] Sent to {to_email}")
