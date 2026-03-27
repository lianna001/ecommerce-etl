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
    st.markdown("**질문 예시**")
    examples = [
        "어제 매출 개요 알려줘",
        "이번 주 채널별 ROAS 비교해줘",
        "지난 2주간 매출이 왜 변했어?",
        "카테고리별 매출 비중 보여줘",
        "FREESHIP 프로모 효과 어때?",
        "최근 14일 매출 트렌드 보여줘",
    ]
    for ex in examples:
        if st.button(ex, use_container_width=True):
            st.session_state.pending_input = ex

    st.divider()
    if st.button("대화 초기화", use_container_width=True):
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
