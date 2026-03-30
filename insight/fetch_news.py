import os
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from datetime import date, timedelta

def _client(): return OpenAI(api_key=os.environ["OPENAI_API_KEY"])

KEYWORDS = ["ecommerce", "online retail", "Coupang", "amazon", "D2C"]


def fetch_google_news(keyword: str, after: str, max_results: int = 3) -> list[str]:
    url = f"https://news.google.com/rss/search?q={keyword}+after:{after}&hl=en-US&gl=US&ceid=US:en"
    resp = requests.get(url, timeout=10)
    soup = BeautifulSoup(resp.content, "xml")
    items = soup.find_all("item")[:max_results]
    return [f"{item.title.text} - {item.source.text}" for item in items if item.title]


def fetch_and_summarize_news() -> str:
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    headlines = []
    for keyword in KEYWORDS:
        try:
            headlines.extend(fetch_google_news(keyword, after=yesterday))
        except Exception as e:
            print(f"[News fetch failed] {keyword}: {e}")

    if not headlines:
        return "No news available for yesterday."

    headlines = list(dict.fromkeys(headlines))[:15]

    prompt = f"""
You are an ecommerce business analyst.
Below are yesterday's ({yesterday}) ecommerce news headlines.
Select the 3 most important ones from a business perspective and summarize each in one sentence.

{chr(10).join(f'- {h}' for h in headlines)}

Format:
- [Headline] : One-line key takeaway
"""

    response = _client().chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.5
    )

    news = response.choices[0].message.content
    print(f"News summary complete ({len(headlines)} headlines → 3 selected)")
    return news
