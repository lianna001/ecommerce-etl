import os
import json
from openai import OpenAI

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])


def generate_insight(summary: dict) -> str:

    prompt = f"""
당신은 이커머스 데이터 분석 전문가입니다.
아래 데이터를 바탕으로 비즈니스 인사이트 리포트를 작성하세요.

날짜: {summary['date']}

[전체 현황]
- 매출: ${summary['overview']['revenue']}
- 주문 수: {summary['overview']['orders']}건
- 방문 유저: {summary['overview']['users']}명
- 전주 대비 매출 변화: {summary['overview']['revenue_change_pct']}%

[카테고리별 매출]
{json.dumps(summary['categories'], ensure_ascii=False, indent=2)}

[채널별 ROAS]
{json.dumps(summary['channels'], ensure_ascii=False, indent=2)}

[프로모션 효과]
{json.dumps(summary['promos'], ensure_ascii=False, indent=2)}

다음 형식으로 작성하세요:
1. 오늘의 핵심 요약 (3줄 이내)
2. 주목할 점 (긍정/부정 각 1~2개)
3. 내일을 위한 액션 아이템 (2~3개)

날카롭고 구체적으로, 숫자 근거를 반드시 포함하세요.
"""

    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[{"role": "user", "content": prompt}],
        temperature=0.7
    )

    insight = response.choices[0].message.content
    print(f"[{summary['date']}] 인사이트 생성 완료")
    return insight