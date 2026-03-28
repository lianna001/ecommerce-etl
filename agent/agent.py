import json
import os
from datetime import date
from openai import OpenAI
from agent.tools import TOOL_SCHEMAS, TOOL_MAP

client = OpenAI(api_key=os.environ["OPENAI_API_KEY"])

SYSTEM_PROMPT = """You are an expert e-commerce data analyst with direct access to Snowflake query tools.
Today's date is {today}.

The database contains order-level data with these columns:
order_id, order_date, user_id, region, product_id, category, amount, promo_code, ad_channel, ad_spend

Categories: Electronics, Clothing, Home & Garden, Sports, Beauty
Ad channels: Google, Meta, TikTok, Email, Organic
Regions: Northeast, Southeast, Midwest, West, Southwest
Promo codes: FREESHIP, DISCOUNT10 (or empty = no promo)

When answering:
- Always query data before answering. Never guess numbers.
- For "why" questions, call compare_two_periods + get_channel_performance + get_category_breakdown to find the root cause.
- Cite specific numbers and % changes from the data.
- Respond in the same language the user uses (Korean or English).
- Be concise and data-driven.
"""


def run_agent(messages: list, on_tool_call=None) -> str:
    """
    OpenAI function calling 루프.
    messages 리스트를 직접 변경(mutate)합니다.
    on_tool_call(name, args): 툴 호출 시 콜백 (UI 상태 업데이트용)
    """
    while True:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=messages,
            tools=TOOL_SCHEMAS,
            tool_choice="auto",
        )

        msg = response.choices[0].message

        # assistant 메시지를 히스토리에 추가
        assistant_msg = {"role": "assistant", "content": msg.content}
        if msg.tool_calls:
            assistant_msg["tool_calls"] = [
                {
                    "id": tc.id,
                    "type": "function",
                    "function": {"name": tc.function.name, "arguments": tc.function.arguments},
                }
                for tc in msg.tool_calls
            ]
        messages.append(assistant_msg)

        # 툴 호출이 없으면 최종 응답 반환
        if not msg.tool_calls:
            return msg.content

        # 툴 실행
        for tc in msg.tool_calls:
            fn_name = tc.function.name
            fn_args = json.loads(tc.function.arguments)

            if on_tool_call:
                on_tool_call(fn_name, fn_args)

            fn = TOOL_MAP.get(fn_name)
            if fn:
                try:
                    result = fn(**fn_args)
                except Exception as e:
                    result = {"error": str(e)}
            else:
                result = {"error": f"Unknown tool: {fn_name}"}

            messages.append({
                "role": "tool",
                "tool_call_id": tc.id,
                "content": json.dumps(result, ensure_ascii=False, default=str),
            })


def build_initial_messages() -> list:
    return [{"role": "system", "content": SYSTEM_PROMPT.format(today=date.today().isoformat())}]
