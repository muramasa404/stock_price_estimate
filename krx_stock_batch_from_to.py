import subprocess
import sys, os
from datetime import datetime
import pymysql

# 🔹 DB 연결 설정
DB_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': '',
    'db': 'stock',
    'charset': 'utf8'
}

# 🔹 날짜 유효성 검사
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYYMMDD.")
        return False

# 🔹 work_day 기준으로 날짜 리스트 조회
def get_work_days(from_date, to_date):
    try:
        conn = pymysql.connect(**DB_CONFIG)
        with conn.cursor() as cursor:
            sql = """
                SELECT work_day
                FROM tb_work_day
                WHERE work_day BETWEEN %s AND %s
                ORDER BY work_day;
            """
            cursor.execute(sql, (from_date, to_date))
            return [row[0] for row in cursor.fetchall()]
    finally:
        conn.close()

# 🔹 실행 환경 설정
venv_python_path = r"D:/python_proj/venv_stock/Scripts/python.exe"
SCRIPT_DIR = r"d:/python_proj/venv_stock/stock"

# 🔹 실행할 스크립트 목록
scripts = [
    ("krx_stock_init.py", True),
    ("krx_tb_stock_day_price_2.py", True),
    ("krx_tb_inv_net_buy_day_2.py", True),
    ("krx_tb_stock_inv_trx_m.py", True),
    ("krx_tb_stock_inv_trx_cnt_2.py", True),
]

# 🔹 실행 날짜 선택
range_mode = ""
while range_mode not in ["1", "2"]:
    print("\n[날짜 실행 방식 선택]")
    print("1. 특정일만 실행")
    print("2. from_date ~ to_date 기간 실행 (work_day 기준)")
    range_mode = input("선택 (1 또는 2): ").strip()

date_list = []

if range_mode == "1":
    while True:
        base_dt = input("Enter base date (YYYYMMDD): ").strip()
        if validate_date(base_dt):
            # tb_work_day에 존재하는지 확인
            date_list = get_work_days(base_dt, base_dt)
            if not date_list:
                print(f"{base_dt} 은(는) work_day 테이블에 존재하지 않습니다. 종료합니다.")
                sys.exit(0)
            break
else:
    while True:
        from_date = input("Enter from_date (YYYYMMDD): ").strip()
        to_date = input("Enter to_date (YYYYMMDD): ").strip()
        if validate_date(from_date) and validate_date(to_date):
            if from_date > to_date:
                print("from_date는 to_date보다 이전이어야 합니다.")
                continue
            date_list = get_work_days(from_date, to_date)
            if not date_list:
                print("해당 구간에 work_day가 없습니다. 종료합니다.")
                sys.exit(0)
            break
        print("날짜 형식 오류. 다시 입력해주세요.")

# 🔹 실행 모드 선택
mode = ""
while mode not in ["1", "2"]:
    print("\n[실행 모드 선택]")
    print("1. 전체 스크립트 한번에 실행")
    print("2. 단계별로 선택 실행")
    mode = input("선택 (1 또는 2): ").strip()

print("\n[✔] 실행 시작\n")

# 🔹 날짜별 배치 실행
for date_str in date_list:
    print(f"\n=== [날짜: {date_str}] ===")

    # krx_tb_work_day_3.py 항상 선실행
    work_day_script = os.path.join(SCRIPT_DIR, "krx_tb_work_day_3.py")
    cmd = [venv_python_path, work_day_script]
    print(f"[▶] work_day 스크립트 실행: {' '.join(cmd)}")
    subprocess.run(cmd)

    for script_name, use_date in scripts:
        if mode == "2":
            user_input = ""
            while user_input not in ["y", "n"]:
                user_input = input(f"▶ {script_name} 실행할까요? (y/n): ").strip().lower()
            if user_input == "n":
                print(f"⏩ {script_name} 건너뜀\n")
                continue

        args = [date_str] if use_date else []
        script_path = os.path.join(SCRIPT_DIR, script_name)
        cmd = [venv_python_path, script_path] + args

        print(f"[▶] 실행 중: {' '.join(cmd)}")
        try:
            subprocess.run(cmd, check=True)
            print(f"[✔] 완료: {script_name}\n")
        except subprocess.CalledProcessError as e:
            print(f"[✖] 실패: {script_name} - 에러 코드 {e.returncode}")
            print(f"    => 무시하고 다음 스크립트 또는 날짜로 계속 진행합니다.\n")
            continue

