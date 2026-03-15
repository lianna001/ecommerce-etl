"""
backfill.py
-----------
3/1부터 오늘(또는 지정 날짜)까지 데이터를 한 번에 생성.
포트폴리오 시작할 때 딱 한 번만 실행하면 됨.

사용법:
    python backfill.py                        # 3/1 ~ 오늘
    python backfill.py 2025-03-01 2025-04-05  # 날짜 범위 지정
"""

import subprocess
import sys
from datetime import date, timedelta


def backfill(start: date, end: date):
    current = start
    success, failed = 0, []

    while current <= end:
        result = subprocess.run(
            ["python3", "extract/generate_daily_data.py", current.strftime("%Y-%m-%d")],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            print(result.stdout.strip())
            success += 1
        else:
            print(f"[ERROR] {current}: {result.stderr.strip()}")
            failed.append(current)

        current += timedelta(days=1)

    print(f"\n백필 완료: {success}일 성공" + (f", {len(failed)}일 실패: {failed}" if failed else ""))


if __name__ == "__main__":
    if len(sys.argv) == 3:
        start = date.fromisoformat(sys.argv[1])
        end   = date.fromisoformat(sys.argv[2])
    else:
        start = date(2025, 3, 1)
        end   = date.today()

    print(f"백필 시작: {start} ~ {end}\n")
    backfill(start, end)
