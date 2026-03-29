import os
import requests
from bs4 import BeautifulSoup
from openai import OpenAI
from datetime import date, timedelta
from email.utils import parsedate_to_datetime

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

KEYWORDS = ["ecommerce", "online retail", "Coupang", "amazon", "D2C"]


def fetch_google_news(keyword: str, after: str, max_results: int = 4) -> list[dict]:
    url = f"https://news.google.com/rss/search?q={keyword}+after:{after}&hl=en-US&gl=US&ceid=US:en"
    resp = requests.get(url, timeout=10)
    soup = BeautifulSoup(resp.content, "xml")
    items = soup.find_all("item")[:max_results]

    results = []
    for item in items:
        if not item.title:
            continue
        try:
            pub_date = parsedate_to_datetime(item.pubDate.text).strftime("%Y-%m-%d") if item.pubDate else "unknown"
        except Exception:
            pub_date = "unknown"
        results.append({
            "title":  item.title.text,
            "url":    item.link.next_sibling.strip() if item.link else "",
            "date":   pub_date,
            "source": item.source.text if item.source else "",
        })
    return results


def fetch_and_summarize_news() -> str:
    yesterday = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

    articles = []
    seen_titles = set()
    for keyword in KEYWORDS:
        try:
            for a in fetch_google_news(keyword, after=yesterday):
                if a["title"] not in seen_titles:
                    seen_titles.add(a["title"])
                    articles.append(a)
        except Exception as e:
            print(f"[News fetch failed] {keyword}: {e}")

    if not articles:
        return "No news available."

    articles = articles[:20]
    article_list = "\n".join(
        f"{i+1}. [{a['date']}] {a['title']} ({a['source']})\n   URL: {a['url']}"
        for i, a in enumerate(articles)
    )

    prompt = f"""You are a sharp ecommerce business analyst. Today is {yesterday}.

From the articles below, select exactly 3 that are most business-relevant.
Strict criteria:
- MUST contain specific numbers (%, $, units, growth rate, etc.)
- Skip vague opinion pieces or articles without concrete data
- Prefer articles about market moves, earnings, sales figures, or strategy shifts with measurable impact

For each selected article, output EXACTLY this format (no extra text):
1. [YYYY-MM-DD] Headline (Source)
   Summary: One sentence with the key number/metric front and center.
   URL: <url>

Articles:
{article_list}
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.2,
    )

    news = response.choices[0].message.content
    print(f"News summary complete ({len(articles)} candidates → 3 selected)")
    return news
