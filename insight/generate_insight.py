import os
import json
from openai import OpenAI

def _client(): return OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def generate_insight(summary: dict) -> str:

    prompt = f"""
You are an ecommerce data analyst.
Based on the data below, write a concise business insight report.

Date: {summary['date']}

[Overview]
- Revenue: ${summary['overview']['revenue']}
- Orders: {summary['overview']['orders']}
- Unique Users: {summary['overview']['users']}
- Revenue Change vs Last Week: {summary['overview']['revenue_change_pct']}%

[Revenue by Category]
{json.dumps(summary['categories'], ensure_ascii=False, indent=2)}

[ROAS by Channel]
{json.dumps(summary['channels'], ensure_ascii=False, indent=2)}

[Promo Code Performance]
{json.dumps(summary['promos'], ensure_ascii=False, indent=2)}

Write in the following format:
1. Today's Key Summary (max 3 lines)
2. Notable Points (1-2 positive, 1-2 negative, with specific numbers)

Be sharp and data-driven. Always back up observations with numbers.
"""

    response = _client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    insight = response.choices[0].message.content
    print(f"[{summary['date']}] Insight generation complete")
    return insight