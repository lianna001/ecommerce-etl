import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

GMAIL_USER     = os.environ["GMAIL_USER"]
GMAIL_PASSWORD = os.environ["GMAIL_APP_PASSWORD"]
TO_EMAIL       = os.environ.get("REPORT_TO_EMAIL", GMAIL_USER)


def build_html_email(date_str: str, insight: str, news: str) -> str:
    prompt = f"""
You are an expert email designer.
Create a clean, professional HTML email report with the following content.
- White background, sans-serif font, mobile-friendly
- Header with date and title "Daily Ecommerce Report"
- Sections divided by horizontal rules
- Easy to read layout
Return only the HTML code, no extra text or markdown.

[Date]
{date_str}

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


def send_daily_report(date_str: str, insight: str, news: str):
    html_body = build_html_email(date_str, insight, news)

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"[Daily Report] {date_str} Ecommerce Insights"
    msg["From"]    = GMAIL_USER
    msg["To"]      = TO_EMAIL
    msg.attach(MIMEText(html_body, "html"))

    with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
        server.login(GMAIL_USER, GMAIL_PASSWORD)
        server.sendmail(GMAIL_USER, TO_EMAIL, msg.as_string())

    print(f"[{date_str}] Email sent to {TO_EMAIL}")