import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

TABLE = "snowflake_learning_db.public.ecommerce_orders"


def detect_anomaly(date_str: str, threshold_pct: float = 25.0) -> dict:
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")

    # 이번 주 합산 (최근 7일, 오늘 포함)
    this_week = hook.get_first(f"""
        SELECT SUM(amount)
        FROM {TABLE}
        WHERE order_date BETWEEN DATEADD(day, -6, '{date_str}') AND '{date_str}'
    """)

    # 지난 주 합산 (그 전 7일)
    last_week = hook.get_first(f"""
        SELECT SUM(amount)
        FROM {TABLE}
        WHERE order_date BETWEEN DATEADD(day, -13, '{date_str}') AND DATEADD(day, -7, '{date_str}')
    """)

    if not this_week or not last_week or not this_week[0] or not last_week[0]:
        return {"is_anomaly": False, "date": date_str}

    this_rev = float(this_week[0])
    last_rev = float(last_week[0])
    change_pct = (this_rev - last_rev) / last_rev * 100

    is_anomaly = abs(change_pct) >= threshold_pct
    severity = "HIGH" if abs(change_pct) >= threshold_pct * 1.5 else "MEDIUM" if is_anomaly else "NORMAL"

    # 채널별 WoW 변화 (원인 파악)
    channels = hook.get_records(f"""
        SELECT
            ad_channel,
            SUM(CASE WHEN order_date BETWEEN DATEADD(day, -6, '{date_str}') AND '{date_str}'
                     THEN amount END) AS this_week,
            SUM(CASE WHEN order_date BETWEEN DATEADD(day, -13, '{date_str}') AND DATEADD(day, -7, '{date_str}')
                     THEN amount END) AS last_week
        FROM {TABLE}
        WHERE order_date BETWEEN DATEADD(day, -13, '{date_str}') AND '{date_str}'
        GROUP BY ad_channel
        ORDER BY ABS(
            SUM(CASE WHEN order_date BETWEEN DATEADD(day, -6, '{date_str}') AND '{date_str}' THEN amount END) -
            SUM(CASE WHEN order_date BETWEEN DATEADD(day, -13, '{date_str}') AND DATEADD(day, -7, '{date_str}') THEN amount END)
        ) DESC NULLS LAST
    """)

    top_movers = [
        {
            "channel": r[0],
            "this_week": round(float(r[1] or 0), 2),
            "last_week": round(float(r[2] or 0), 2),
            "change_pct": round((float(r[1] or 0) - float(r[2] or 0)) / max(float(r[2] or 1), 1) * 100, 1),
        }
        for r in channels[:3]
    ]

    result = {
        "date": date_str,
        "is_anomaly": is_anomaly,
        "severity": severity,
        "this_week_revenue": round(this_rev, 2),
        "last_week_revenue": round(last_rev, 2),
        "change_pct": round(change_pct, 1),
        "top_movers": top_movers,
    }

    print(f"[{date_str}] WoW: ${this_rev:,.0f} vs ${last_rev:,.0f} ({change_pct:+.1f}%) → {'ANOMALY (' + severity + ')' if is_anomaly else 'NORMAL'}")

    if is_anomaly:
        _send_anomaly_alert(result)

    return result


def _send_anomaly_alert(anomaly: dict):
    gmail_user = os.environ.get("GMAIL_USER")
    gmail_pw   = os.environ.get("GMAIL_APP_PASSWORD")
    to_email   = os.environ.get("REPORT_TO_EMAIL", gmail_user)

    if not gmail_user or not gmail_pw:
        print("[Anomaly Alert] Email credentials missing, skipping.")
        return

    direction = "down" if anomaly["change_pct"] < 0 else "up"
    emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"

    movers_text = "\n".join(
        f"  • {m['channel']}: {m['change_pct']:+.1f}% (${m['last_week']:,.0f} → ${m['this_week']:,.0f})"
        for m in anomaly["top_movers"]
    )

    body = f"""
<html><body style="font-family:sans-serif;max-width:600px;margin:auto;padding:24px">
  <h2>{emoji} Weekly Revenue Anomaly — {anomaly['date']}</h2>
  <p>This week's revenue is <strong>{abs(anomaly['change_pct']):.1f}% {direction}</strong> vs last week.</p>
  <table style="border-collapse:collapse;width:100%">
    <tr><td style="padding:8px;background:#f5f5f5"><b>This Week</b></td><td style="padding:8px">${anomaly['this_week_revenue']:,.2f}</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>Last Week</b></td><td style="padding:8px">${anomaly['last_week_revenue']:,.2f}</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>Change</b></td><td style="padding:8px">{anomaly['change_pct']:+.1f}%</td></tr>
    <tr><td style="padding:8px;background:#f5f5f5"><b>Severity</b></td><td style="padding:8px">{anomaly['severity']}</td></tr>
  </table>
  <h3>Top Channel Movers</h3>
  <pre style="background:#f9f9f9;padding:12px;border-radius:4px">{movers_text}</pre>
</body></html>
"""

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"{emoji} [Weekly Alert] Revenue {direction} {abs(anomaly['change_pct']):.0f}% WoW — {anomaly['date']}"
    msg["From"]    = gmail_user
    msg["To"]      = to_email
    msg.attach(MIMEText(body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(gmail_user, gmail_pw)
        server.sendmail(gmail_user, to_email, msg.as_string())

    print(f"[Anomaly Alert] Sent to {to_email}")
