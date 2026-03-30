import os
import requests

NOTION_TOKEN      = os.environ["NOTION_TOKEN"]
NOTION_PARENT_ID  = os.environ["NOTION_PARENT_PAGE_ID"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28",
}


def _paragraph(text: str) -> dict:
    return {
        "object": "block",
        "type": "paragraph",
        "paragraph": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }


def _heading2(text: str) -> dict:
    return {
        "object": "block",
        "type": "heading_2",
        "heading_2": {"rich_text": [{"type": "text", "text": {"content": text}}]},
    }


def _divider() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}


def _callout(text: str, emoji: str) -> dict:
    return {
        "object": "block",
        "type": "callout",
        "callout": {
            "rich_text": [{"type": "text", "text": {"content": text}}],
            "icon": {"type": "emoji", "emoji": emoji},
            "color": "red_background" if emoji == "🔴" else "yellow_background",
        },
    }


def text_to_blocks(text: str) -> list:
    blocks = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if line:
            blocks.append(_paragraph(line))
    return blocks


def update_notion_page(date_str: str, insight: str, news: str, anomaly: dict = None):
    blocks = []

    # 이상 감지 섹션 (감지된 경우만)
    if anomaly and anomaly.get("is_anomaly"):
        emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"
        anomaly_text = (
            f"Severity: {anomaly['severity']}  |  "
            f"This week: ${anomaly['this_week_revenue']:,.0f}  →  "
            f"Last week: ${anomaly['last_week_revenue']:,.0f}  ({anomaly['change_pct']:+.1f}% WoW)\n"
            + "\n".join(
                f"• {m['channel']}: {m['change_pct']:+.1f}%  (${m['last_week']:,.0f} → ${m['this_week']:,.0f})"
                for m in anomaly.get("top_movers", [])
            )
        )
        blocks.append(_callout(anomaly_text, emoji))
        blocks.append(_divider())

    # 데이터 인사이트
    blocks.append(_heading2("📊 Data Insights"))
    blocks.extend(text_to_blocks(insight))
    blocks.append(_divider())

    # 뉴스
    blocks.append(_heading2("📰 Ecommerce News"))
    blocks.extend(text_to_blocks(news))

    # 새 서브페이지 생성
    payload = {
        "parent": {"page_id": NOTION_PARENT_ID},
        "properties": {
            "title": {
                "title": [{"type": "text", "text": {"content": f"Daily Report — {date_str}"}}]
            }
        },
        "children": blocks,
    }

    resp = requests.post("https://api.notion.com/v1/pages", headers=HEADERS, json=payload)

    if resp.status_code == 200:
        print(f"[{date_str}] Notion page created successfully")
    else:
        print(f"[{date_str}] Notion failed: {resp.status_code} {resp.text}")
