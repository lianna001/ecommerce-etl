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


def update_notion_page(date_str: str, insight: str, news: str):
    page_id = NOTION_PAGE_ID

    # 기존 내용 삭제
    clear_page_content(page_id)

    # 새 블록 구성
    blocks = []

    # 날짜 헤더
    blocks.append({
        "object": "block",
        "type": "heading_1",
        "heading_1": {
            "rich_text": [{"type": "text", "text": {"content": f"Daily Ecommerce Report — {date_str}"}}]
        }
    })

    # 구분선
    blocks.append({"object": "block", "type": "divider", "divider": {}})

    # 인사이트 섹션
    blocks.append({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": "📊 Data Insights"}}]
        }
    })
    blocks.extend(text_to_blocks(insight))

    # 구분선
    blocks.append({"object": "block", "type": "divider", "divider": {}})

    # 뉴스 섹션
    blocks.append({
        "object": "block",
        "type": "heading_2",
        "heading_2": {
            "rich_text": [{"type": "text", "text": {"content": "📰 Ecommerce News"}}]
        }
    })
    blocks.extend(text_to_blocks(news))

    # 페이지에 블록 추가
    url = f"https://api.notion.com/v1/blocks/{page_id}/children"
    resp = requests.patch(url, headers=HEADERS, json={"children": blocks})

    if resp.status_code == 200:
        print(f"[{date_str}] Notion page updated successfully")
    else:
        print(f"[{date_str}] Notion update failed: {resp.status_code} {resp.text}")