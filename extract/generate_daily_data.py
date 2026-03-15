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

# 프로모션 코드 (없음 포함)
PROMO_CODES = [None, None, None, "SAVE10", "SAVE20", "FREESHIP", "FLASH30"]

# 광고 채널
AD_CHANNELS = ["Google", "Meta", "TikTok", "Email", "Organic"]

# 지역
REGIONS = ["Northeast", "Southeast", "Midwest", "West", "Southwest"]

# ── 데이터 생성 함수 ────────────────────────────────────
def generate_orders(target_date: date) -> pd.DataFrame:
    """하루치 주문 데이터 생성"""

    # 날짜를 seed로 사용 → 같은 날짜는 항상 같은 데이터
    seed = RANDOM_SEED_BASE + target_date.toordinal()
    np.random.seed(seed)
    random.seed(seed)

    n = DAILY_ORDER_COUNT + random.randint(-50, 50)

    # 주말엔 주문 20% 증가
    if target_date.weekday() >= 5:
        n = int(n * 1.2)

    # 월말 프로모션 효과 (25일 이후 주문 30% 증가)
    if target_date.day >= 25:
        n = int(n * 1.3)

    categories  = random.choices(list(CATEGORIES.keys()), k=n)
    promo_codes = random.choices(PROMO_CODES, k=n)

    amounts = []
    for cat, promo in zip(categories, promo_codes):
        low, high = CATEGORIES[cat]
        base = round(np.random.uniform(low, high), 2)
        # 프로모션 할인 적용
        if promo == "SAVE10":   base = round(base * 0.9, 2)
        elif promo == "SAVE20": base = round(base * 0.8, 2)
        elif promo == "FLASH30":base = round(base * 0.7, 2)
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
        "ad_channel": random.choices(AD_CHANNELS, k=n),
        "ad_spend":   [round(np.random.uniform(0.5, 5.0), 2) for _ in range(n)],
    })

    return df


def append_to_csv(df: pd.DataFrame, path: str):
    """기존 파일에 누적. 없으면 새로 생성. 중복 날짜는 덮어쓰기."""
    os.makedirs(os.path.dirname(path), exist_ok=True)

    if os.path.exists(path):
        existing = pd.read_csv(path)
        # 같은 날짜 데이터가 이미 있으면 제거 후 재적재 (멱등성 보장)
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

    # 날짜 인자 처리 (Airflow는 {{ ds }} 로 날짜 넘김)
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
