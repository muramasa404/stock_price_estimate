
"""
[실행 모드 선택]
1. 전체 스크립트 한번에 실행
2. 단계별로 선택 실행
선택 (1 또는 2): 2

▶ krx_stock_init.py 실행할까요? (y/n): y
▶ krx_tb_work_day_3.py 실행할까요? (y/n): n

"""

import subprocess
import sys, os
from datetime import datetime

# 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False

# 🔹 날짜 인자 처리
if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
    base_dt = sys.argv[1]
else:
    while True:
        base_dt = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
        if validate_date(base_dt):
            break
        print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")

# 🔹 가상환경의 Python 경로
venv_python_path = r"D:/python_proj/venv_stock/Scripts/python.exe"

# 🔹 스크립트 디렉토리 설정
SCRIPT_DIR = r"d:/python_proj/venv_stock/stock"

# 🔹 실행할 스크립트 및 인자 설정
scripts = [
    ("krx_stock_init.py", [base_dt]),
    ("krx_tb_work_day_4.py", []),
    ("krx_tb_stock_day_price_2.py", [base_dt]),
    ("krx_stock_idx_calc.py", [base_dt]),
    ("krx_tb_inv_net_buy_day_2.py", [base_dt]),
    ("krx_tb_stock_inv_trx_m.py", [base_dt]),
    ("krx_tb_stock_inv_trx_cnt_3.py", [base_dt]),
]

# 🔹 실행 방식 선택
mode = ""
while mode not in ["1", "2"]:
    print("\n[실행 모드 선택]")
    print("1. 전체 스크립트 한번에 실행")
    print("2. 단계별로 선택 실행")
    mode = input("선택 (1 또는 2): ").strip()

print("\n[✔] 실행 시작\n")

# 🔹 실행 루프
for script_name, args in scripts:
    if mode == "2":
        user_input = ""
        while user_input not in ["y", "n"]:
            user_input = input(f"▶ {script_name} 실행할까요? (y/n): ").strip().lower()
        if user_input == "n":
            print(f"⏩ {script_name} 건너뜀\n")
            continue

    script_path = os.path.join(SCRIPT_DIR, script_name)
    cmd = [venv_python_path, script_path] + args

    print(f"[▶] 실행 중: {' '.join(cmd)}")
    try:
        subprocess.run(cmd, check=True)
        print(f"[✔] 완료: {script_name}\n")
    except subprocess.CalledProcessError as e:
        print(f"[✖] 실패: {script_name} - 에러 코드 {e.returncode}")
        sys.exit(e.returncode)

