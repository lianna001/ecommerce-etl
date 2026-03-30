import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

GMAIL_USER     = os.environ["GMAIL_USER"]
GMAIL_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
TO_EMAIL       = os.environ.get("REPORT_TO_EMAIL", GMAIL_USER)


def _format_anomaly(anomaly: dict) -> str:
    if not anomaly or not anomaly.get("is_anomaly"):
        return ""
    emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"
    movers = "\n".join(
        f"  • {m['channel']}: {m['change_pct']:+.1f}% (${m['last_week']:,.0f} → ${m['this_week']:,.0f})"
        for m in anomaly.get("top_movers", [])
    )
    return (
        f"{emoji} WEEKLY ANOMALY DETECTED ({anomaly['severity']})\n"
        f"This week: ${anomaly['this_week_revenue']:,.2f} vs Last week: ${anomaly['last_week_revenue']:,.2f} ({anomaly['change_pct']:+.1f}% WoW)\n"
        f"Top channel movers:\n{movers}"
    )


def build_html_email(date_str: str, insight: str, news: str, anomaly: dict = None) -> str:
    anomaly_section = f"\n[⚠️ Anomaly Alert]\n{_format_anomaly(anomaly)}\n" if anomaly and anomaly.get("is_anomaly") else ""

    prompt = f"""
You are an expert email designer.
Create a clean, professional HTML email report with the following content.
- White background, sans-serif font, mobile-friendly
- DO NOT add any title or date header at the top — the content below already contains its own header
- Sections divided by horizontal rules
- Easy to read layout
- If an Anomaly Alert section exists, highlight it with a colored background (red for HIGH, yellow for MEDIUM)
Return only the HTML code, no extra text or markdown.
{anomaly_section}
[Data Insights]
{insight}

[Today's Ecommerce News]
{news}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.3
    )

    return response.choices[0].message.content


def send_daily_report(date_str: str, insight: str, news: str, anomaly: dict = None):
    html_body = build_html_email(date_str, insight, news, anomaly=anomaly)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Daily Report] {date_str} Ecommerce Insights"
    msg["From"]    = GMAIL_USER
    msg["To"]      = TO_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())

    print(f"[{date_str}] Email sent to {TO_EMAIL}")
