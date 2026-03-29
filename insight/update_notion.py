import os
import requests
from datetime import date

NOTION_TOKEN   = os.environ["NOTION_TOKEN"]
NOTION_PAGE_ID = os.environ["NOTION_PAGE_ID"]

HEADERS = {
    "Authorization": f"Bearer {NOTION_TOKEN}",
    "Content-Type": "application/json",
    "Notion-Version": "2022-06-28"
}


def clear_page_content(page_id: str):
    """기존 페이지 블록 전체 삭제"""
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    resp = requests.get(url, headers=HEADERS)
    blocks = resp.json().get("results", [])

    for block in blocks:
        requests.delete(
            f"https://api.notion.com/v1/blocks/{block['id']}",
            headers=HEADERS
        )


def text_to_blocks(text: str) -> list:
    """텍스트를 노션 paragraph 블록으로 변환"""
    blocks = []
    for line in text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue
        blocks.append({
            "object": "block",
            "type": "paragraph",
            "paragraph": {
                "rich_text": [{"type": "text", "text": {"content": line}}]
            }
        })
    return blocks


def _heading2(text: str) -> dict:
    return {"object": "block", "type": "heading_2", "heading_2": {"rich_text": [{"type": "text", "text": {"content": text}}]}}

def _divider() -> dict:
    return {"object": "block", "type": "divider", "divider": {}}


def update_notion_page(date_str: str, insight: str, news: str, anomaly: dict = None, forecast: dict = None):
    page_id = NOTION_PAGE_ID
    clear_page_content(page_id)

    blocks = []

    # 헤더
    blocks.append({"object": "block", "type": "heading_1", "heading_1": {
        "rich_text": [{"type": "text", "text": {"content": f"Daily Ecommerce Report — {date_str}"}}]
    }})
    blocks.append(_divider())

    # 이상 감지 섹션 (감지된 경우만)
    if anomaly and anomaly.get("is_anomaly"):
        emoji = "🔴" if anomaly["severity"] == "HIGH" else "🟡"
        blocks.append(_heading2(f"{emoji} Anomaly Alert"))
        anomaly_lines = [
            f"Severity: {anomaly['severity']}",
            f"Today's Revenue: ${anomaly['today_revenue']:,.2f} ({anomaly['change_pct']:+.1f}% vs 7d avg)",
            "Top Channel Movers:",
        ] + [f"  • {m['channel']}: {m['change_pct']:+.1f}%" for m in anomaly.get("top_movers", [])]
        blocks.extend(text_to_blocks("\n".join(anomaly_lines)))
        blocks.append(_divider())

    # 데이터 인사이트
    blocks.append(_heading2("📊 Data Insights"))
    blocks.extend(text_to_blocks(insight))
    blocks.append(_divider())

    # 7일 예측 섹션
    if forecast and forecast.get("forecast"):
        blocks.append(_heading2("📈 7-Day Revenue Forecast"))
        forecast_lines = [f"Trend: {forecast['trend'].upper()} | 28d avg: ${forecast['avg_28d']:,.0f}"] + [
            f"{f['day']} {f['date']}: ${f['predicted_revenue']:,.0f}" for f in forecast["forecast"]
        ]
        blocks.extend(text_to_blocks("\n".join(forecast_lines)))
        blocks.append(_divider())

    # 뉴스
    blocks.append(_heading2("📰 Ecommerce News"))
    blocks.extend(text_to_blocks(news))

    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    resp = requests.patch(url, headers=HEADERS, json={"children": blocks})

    if resp.status_code == 200:
        print(f"[{date_str}] Notion page updated successfully")
    else:
        print(f"[{date_str}] Notion update failed: {resp.status_code} {resp.text}")