"""
generate_daily_data.py
----------------------
실행할 때마다 '오늘 날짜' 데이터를 생성해서 CSV에 누적 저장.
Airflow에서 매일 자동 실행하면 3/1부터 데이터가 쌓임.

직접 테스트할 때:
    python generate_daily_data.py              # 오늘 날짜로 생성
    python generate_daily_data.py 2025-03-01   # 특정 날짜로 생성 (백필용)
"""

import pandas as pd
import numpy as np
import random
import sys
import os
from datetime import datetime, date

# ── 설정 ──────────────────────────────────────────────
OUTPUT_PATH = "data/sample/orders.csv"
DAILY_ORDER_COUNT = 200          # 하루 주문 수 (±50 랜덤)
RANDOM_SEED_BASE  = 42           # 날짜별로 seed 고정 → 같은 날 실행해도 동일 결과

# 상품 카테고리 & 가격대
CATEGORIES = {
    "Electronics":  (50,  500),
    "Clothing":     (20,  150),
    "Home & Garden":(15,  200),
    "Sports":       (25,  300),
    "Beauty":       (10,  100),
}

# 프로모션 코드: FREESHIP / DISCOUNT10 두 개만
# 다이나믹 분포: 주중엔 프로모션 적게, 주말·월말엔 많이
PROMO_POOL_NORMAL  = [None] * 65 + ["FREESHIP"] * 20 + ["DISCOUNT10"] * 15
PROMO_POOL_WEEKEND = [None] * 45 + ["FREESHIP"] * 30 + ["DISCOUNT10"] * 25
PROMO_POOL_MONTHEND= [None] * 30 + ["FREESHIP"] * 35 + ["DISCOUNT10"] * 35  # 월말 프로모션 heavy

# 광고 채널 가중치: Meta 대폭↑, Email <5%, Organic 갭 확대
# [Google, Meta, TikTok, Email, Organic]
AD_CHANNELS = ["Google", "Meta", "TikTok", "Email", "Organic"]
AD_WEIGHTS  = [0.22,     0.38,   0.18,     0.04,    0.18]   # Email 4%, Meta 38%

# 지역
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]

# ── 카테고리 주차별 high-growth 패턴 ──────────────────
# 주차(1~4)마다 성장세가 다른 카테고리를 지정
# 해당 카테고리는 주문 비중을 boost
WEEKLY_HOT_CATEGORY = {
    1: "Electronics",    # 1주차: 전자기기 수요 높음
    2: "Clothing",       # 2주차: 의류 시즌 전환
    3: "Sports",         # 3주차: 스포츠 캠페인 시즌
    4: "Beauty",         # 4주차: 월말 뷰티/셀프케어
}
HOT_BOOST = 1.8          # hot category 비중 배율

# ── 시즌 계수 (월별) ────────────────────────────────
# 1~12월 매출 계수: 연말 성수기, 여름 성수기, 2월 비수기 반영
MONTHLY_SEASON = {
    1:  0.85,   # 1월: 연초 소비 위축
    2:  0.78,   # 2월: 비수기
    3:  0.90,
    4:  0.95,
    5:  1.00,
    6:  1.05,   # 여름 시작
    7:  1.10,   # 여름 성수기
    8:  1.08,
    9:  0.95,   # 가을 전환기
    10: 1.05,   # 핼러윈/추석 효과
    11: 1.30,   # 블랙프라이데이
    12: 1.40,   # 크리스마스 성수기
}


# ── 데이터 생성 함수 ────────────────────────────────────
def generate_orders(target_date: date) -> pd.DataFrame:
    """하루치 주문 데이터 생성"""

    seed = RANDOM_SEED_BASE + target_date.toordinal()
    np.random.seed(seed)
    random.seed(seed)

    n = DAILY_ORDER_COUNT + random.randint(-50, 50)

    # ── 주문 수 보정 ──
    # 주말 boost
    is_weekend = target_date.weekday() >= 5
    if is_weekend:
        n = int(n * 1.2)

    # 시즌 계수 적용 (주문 수에도 반영)
    season_factor = MONTHLY_SEASON.get(target_date.month, 1.0)
    n = int(n * season_factor)

    # 월말 프로모션 효과
    is_monthend = target_date.day >= 25
    if is_monthend:
        n = int(n * 1.3)

    # ── 카테고리 가중치 (주차별 hot category boost) ──
    week_of_month = min((target_date.day - 1) // 7 + 1, 4)
    hot_cat = WEEKLY_HOT_CATEGORY.get(week_of_month)
    cat_weights = []
    for cat in CATEGORIES:
        cat_weights.append(HOT_BOOST if cat == hot_cat else 1.0)
    total_w = sum(cat_weights)
    cat_weights = [w / total_w for w in cat_weights]

    categories = random.choices(list(CATEGORIES.keys()), weights=cat_weights, k=n)

    # ── 프로모션 풀 선택 ──
    if is_monthend:
        promo_pool = PROMO_POOL_MONTHEND
    elif is_weekend:
        promo_pool = PROMO_POOL_WEEKEND
    else:
        promo_pool = PROMO_POOL_NORMAL
    promo_codes = random.choices(promo_pool, k=n)

    # ── 광고 채널 & ad_spend ──
    # ad_spend는 채널·시즌에 따라 다르게, 매출과 완전 연동 안 되도록 노이즈 추가
    ad_channel_list = random.choices(AD_CHANNELS, weights=AD_WEIGHTS, k=n)

    channel_base_spend = {
        "Google":  (1.5, 6.0),
        "Meta":    (1.0, 5.5),
        "TikTok":  (0.8, 4.0),
        "Email":   (0.1, 0.8),   # 이메일은 단가 낮음
        "Organic": (0.0, 0.0),   # 오가닉은 광고비 없음
    }
    ad_spends = []
    for ch in ad_channel_list:
        lo, hi = channel_base_spend[ch]
        if hi == 0:
            ad_spends.append(0.0)
        else:
            # 광고비에 독립적인 노이즈 → 매출과 갭 발생
            spend = round(np.random.uniform(lo, hi) * np.random.uniform(0.6, 1.4), 2)
            ad_spends.append(spend)

    # ── 금액 계산 ──
    amounts = []
    for cat, promo in zip(categories, promo_codes):
        low, high = CATEGORIES[cat]
        base = round(np.random.uniform(low, high) * season_factor, 2)
        if promo == "DISCOUNT10": base = round(base * 0.90, 2)
        # FREESHIP은 금액 변동 없음 (배송비 무료만 적용)
        amounts.append(base)

    df = pd.DataFrame({
        "order_id":   [f"ORD-{target_date.strftime('%Y%m%d')}-{str(i).zfill(4)}" for i in range(1, n+1)],
        "order_date": target_date.strftime("%Y-%m-%d"),
        "user_id":    [f"USR-{random.randint(1000, 9999)}" for _ in range(n)],
        "region":     random.choices(REGIONS, k=n),
        "product_id": [f"PROD-{random.randint(100, 999)}" for _ in range(n)],
        "category":   categories,
        "amount":     amounts,
        "promo_code": [p if p else "" for p in promo_codes],
        "ad_channel": ad_channel_list,
        "ad_spend":   ad_spends,
    })

    return df


def append_to_csv(df: pd.DataFrame, path: str):
    """기존 파일에 누적. 없으면 새로 생성. 중복 날짜는 덮어쓰기."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        existing = pd.read_csv(path)
        target_date = df["order_date"].iloc[0]
        existing = existing[existing["order_date"] != target_date]
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.sort_values("order_date").reset_index(drop=True)
    else:
        combined = df

    combined.to_csv(path, index=False)
    return len(combined)


# ── 실행 ───────────────────────────────────────────────
if __name__ == "__main__":

    if len(sys.argv) > 1:
        target_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    else:
        target_date = date.today()

    print(f"[generate] 날짜: {target_date}")

    df = generate_orders(target_date)
    total_rows = append_to_csv(df, OUTPUT_PATH)

    print(f"[generate] 오늘 주문 수:  {len(df)}건")
    print(f"[generate] 누적 총 주문: {total_rows}건")
    print(f"[generate] 저장 경로:    {OUTPUT_PATH}")
