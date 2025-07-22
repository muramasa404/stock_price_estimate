

import sys
import time
from datetime import datetime
from sqlalchemy import create_engine, text

# 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False
    

# 🔹 Argument에서 base_dt 받기 또는 사용자 입력 받기
if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
    base_dt = sys.argv[1]
else:
    while True:
        base_dt = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
        if validate_date(base_dt):
            break
        print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")


# MySQL 연결 설정
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"

# SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8")

# 삭제 SQL
del_a_sql = text("DELETE FROM stock.tb_inv_net_buy_day  WHERE 1=1  AND BASE_DT = :base_dt")
del_b_sql = text("DELETE FROM stock.tb_stock_inv_trx_m WHERE BASE_DT = :base_dt")
del_c_sql = text("DELETE FROM stock.tb_stock_inv_trx_cnt WHERE BASE_DT = :base_dt")

del_d_sql = text("DELETE FROM stock.tb_stock_day_price WHERE BASE_DT = :base_dt")

# 실행
try :
    with engine.begin() as conn:

        print(f"[i] {base_dt} tb_stock_day_price 데이터 삭제 중...",del_d_sql)
        conn.execute(del_d_sql, {'base_dt': base_dt})
        time.sleep(1)
        print(f"[✔] {base_dt} tb_stock_day_price 데이터 삭제 완료")

        
        print(f"[i] {base_dt} tb_inv_net_buy_day 데이터 삭제 중...", del_a_sql)
        conn.execute(del_a_sql, {'base_dt': base_dt}) 
        time.sleep(1)
        print(f"[✔] {base_dt} tb_stock_inv_trx_m 데이터 삭제 완료")

    
        print(f"[i] {base_dt} tb_stock_inv_trx_m 데이터 삭제 중...", del_b_sql)
        conn.execute(del_b_sql, {'base_dt': base_dt}) 
        time.sleep(1)
        print(f"[✔] {base_dt} tb_stock_inv_trx_m 데이터 삭제 완료")


        print(f"[i] {base_dt} tb_stock_inv_trx_cnt 데이터 삭제 중...",del_c_sql)
        conn.execute(del_c_sql, {'base_dt': base_dt})
        time.sleep(1)
        print(f"[✔] {base_dt} tb_stock_inv_trx_cnt 데이터 삭제 완료")



except Exception as e:
        print(f"Error delete : {e}")
