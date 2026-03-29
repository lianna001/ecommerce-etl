import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import json
import streamlit as st
from dotenv import load_dotenv
from agent.agent import run_agent, build_initial_messages

load_dotenv()

st.set_page_config(
    page_title="E-commerce AI Agent",
    page_icon="📊",
    layout="wide",
)

# ── 사이드바 ────────────────────────────────────────────────────────────────
with st.sidebar:
    st.title("📊 E-commerce\nData Analyst")
    st.divider()
    st.markdown("**Example Questions**")
    examples = [
        "Show me yesterday's sales overview",
        "Compare ROAS by channel this week",
        "Why did revenue change over the last 2 weeks?",
        "Break down revenue by category",
        "How effective is the FREESHIP promo?",
        "Show me the revenue trend for the last 14 days",
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state.pending_input = ex

    st.divider()
    if st.button("Clear conversation", use_container_width=True):
        st.session_state.messages = build_initial_messages()
        st.session_state.display_history = []
        st.rerun()

# ── 세션 상태 초기화 ─────────────────────────────────────────────────────────
if "messages" not in st.session_state:
    st.session_state.messages = build_initial_messages()

if "display_history" not in st.session_state:
    st.session_state.display_history = []

# ── 대화 히스토리 렌더링 ─────────────────────────────────────────────────────
for item in st.session_state.display_history:
    with st.chat_message(item["role"]):
        st.markdown(item["content"])
        if item.get("tools_used"):
            with st.expander(f"🔧 사용된 도구 {len(item['tools_used'])}개"):
                for tool in item["tools_used"]:
                    st.code(
                        f"{tool['name']}({json.dumps(tool['args'], ensure_ascii=False)})",
                        language="python"
                    )

# ── 입력 처리 ────────────────────────────────────────────────────────────────
user_input = st.chat_input("예: 어제 매출이 왜 떨어졌나요?")

# 사이드바 예시 버튼 클릭 처리
if "pending_input" in st.session_state:
    user_input = st.session_state.pop("pending_input")

if user_input:
    # 사용자 메시지 표시
    with st.chat_message("user"):
        st.markdown(user_input)
    st.session_state.display_history.append({"role": "user", "content": user_input})
    st.session_state.messages.append({"role": "user", "content": user_input})

    # 에이전트 실행
    with st.chat_message("assistant"):
        tools_used = []
        tool_status = st.empty()

        def on_tool_call(name, args):
            tools_used.append({"name": name, "args": args})
            label = f"🔍 `{name}` 조회 중... ({len(tools_used)}번째 도구)"
            tool_status.info(label)

        response = run_agent(st.session_state.messages, on_tool_call=on_tool_call)
        tool_status.empty()

        st.markdown(response)

        if tools_used:
            with st.expander(f"🔧 사용된 도구 {len(tools_used)}개"):
                for tool in tools_used:
                    st.code(
                        f"{tool['name']}({json.dumps(tool['args'], ensure_ascii=False)})",
                        language="python"
                    )

    st.session_state.display_history.append({
        "role": "assistant",
        "content": response,
        "tools_used": tools_used,
    })
