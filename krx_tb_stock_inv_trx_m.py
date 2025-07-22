
from datetime import datetime
import time
import io
import os

import pandas as pd
from sqlalchemy import create_engine, text
import sys


# 📌 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"[!] Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False
    

# 🔹 Argument에서 base_dt 받기 또는 사용자 입력 받기
if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
    base_date = sys.argv[1]
else:
    while True:
        base_date = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
        if validate_date(base_date):
            break
        print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")




# DB 연결 설정
engine = create_engine("mysql+mysqlconnector://root@localhost/stock?charset=utf8")

# 기준일의 work_seq 값 가져오기
query_seq = f"""
    SELECT work_seq FROM stock.tb_work_day
    WHERE work_day = '{base_date}'
"""
with engine.connect() as conn:
    result = conn.execute(text(query_seq)).fetchone()
    if result is None:
        raise ValueError("기준일의 WORK_SEQ 값을 찾을 수 없습니다.")
    base_seq = result[0]

# 기준일 포함, 과거 6일의 WORK_DAY 가져오기
query_days = f"""
    SELECT work_day FROM stock.tb_work_day
    WHERE work_seq BETWEEN {base_seq - 6} AND {base_seq}
    ORDER BY work_seq DESC
"""

with engine.connect() as conn:
    days = [row[0] for row in conn.execute(text(query_days)).fetchall()]
    if len(days) < 7:
        print(f"[!] 거래일 수가 부족합니다. 최소 7일치 필요하지만 {len(days)}일만 조회되었습니다.")
        print(f"    기준일: {base_date}, 조회된 날짜: {days}")
        sys.exit(1)


# 날짜 매핑
base_dt = days[0]
prev_days = days[1:]  # D2 ~ D7

print( 'base_dt : ', base_dt )
print( 'prev_days : ', prev_days )


# 기 등록된 데이터 삭제 

del_b_sql = text("DELETE FROM stock.tb_stock_inv_trx_m WHERE BASE_DT = :base_dt")

with engine.begin() as conn:

    print(f"[i] {base_dt} tb_stock_inv_trx_m 데이터 삭제 중...", del_b_sql)
    conn.execute(del_b_sql, {'base_dt': base_dt}) 
    time.sleep(1)
    print(f"[✔] {base_dt} tb_stock_inv_trx_m 데이터 삭제 완료")


# ▶ 새 INSERT 쿼리
insert_sql = f"""
    INSERT INTO stock.tb_stock_inv_trx_m (
        BASE_DT, STOCK_CD, STOCK_NM,
        D1A_RANK_AMT, D1B_RANK_AMT,
        D1A_TRADE_NET_BUY_QTY, D1B_TRADE_NET_BUY_QTY,
        D2A_TRADE_NET_BUY_QTY, D2B_TRADE_NET_BUY_QTY,
        D3A_TRADE_NET_BUY_QTY, D3B_TRADE_NET_BUY_QTY,
        D4A_TRADE_NET_BUY_QTY, D4B_TRADE_NET_BUY_QTY,
        D5A_TRADE_NET_BUY_QTY, D5B_TRADE_NET_BUY_QTY,
        D6A_TRADE_NET_BUY_QTY, D6B_TRADE_NET_BUY_QTY,
        D7A_TRADE_NET_BUY_QTY, D7B_TRADE_NET_BUY_QTY
    )
    SELECT
        a.base_dt, a.stock_cd, a.stock_nm,
        SUM(CASE WHEN a.inv_div = '7050' THEN IFNULL(a.rank_amt, 0) ELSE 0 END),
        SUM(CASE WHEN a.inv_div = '9000' THEN IFNULL(a.rank_amt, 0) ELSE 0 END),
        SUM(CASE WHEN a.inv_div = '7050' THEN IFNULL(a.trade_net_buy_qty, 0) ELSE 0 END),
        SUM(CASE WHEN a.inv_div = '9000' THEN IFNULL(a.trade_net_buy_qty, 0) ELSE 0 END),
        IFNULL(b1.trade_net_buy_qty, 0), IFNULL(b2.trade_net_buy_qty, 0),
        IFNULL(c1.trade_net_buy_qty, 0), IFNULL(c2.trade_net_buy_qty, 0),
        IFNULL(d1.trade_net_buy_qty, 0), IFNULL(d2.trade_net_buy_qty, 0),
        IFNULL(e1.trade_net_buy_qty, 0), IFNULL(e2.trade_net_buy_qty, 0),
        IFNULL(f1.trade_net_buy_qty, 0), IFNULL(f2.trade_net_buy_qty, 0),
        IFNULL(g1.trade_net_buy_qty, 0), IFNULL(g2.trade_net_buy_qty, 0)
    FROM (
        SELECT * FROM stock.tb_inv_net_buy_day
        WHERE base_dt = '{base_dt}' AND (rank_amt < 51 OR rank_cnt < 51)
    ) a
    LEFT JOIN stock.tb_inv_net_buy_day b1 ON b1.base_dt = '{prev_days[0]}' AND b1.stock_cd = a.stock_cd AND b1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day b2 ON b2.base_dt = '{prev_days[0]}' AND b2.stock_cd = a.stock_cd AND b2.inv_div = '9000'
    LEFT JOIN stock.tb_inv_net_buy_day c1 ON c1.base_dt = '{prev_days[1]}' AND c1.stock_cd = a.stock_cd AND c1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day c2 ON c2.base_dt = '{prev_days[1]}' AND c2.stock_cd = a.stock_cd AND c2.inv_div = '9000'
    LEFT JOIN stock.tb_inv_net_buy_day d1 ON d1.base_dt = '{prev_days[2]}' AND d1.stock_cd = a.stock_cd AND d1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day d2 ON d2.base_dt = '{prev_days[2]}' AND d2.stock_cd = a.stock_cd AND d2.inv_div = '9000'
    LEFT JOIN stock.tb_inv_net_buy_day e1 ON e1.base_dt = '{prev_days[3]}' AND e1.stock_cd = a.stock_cd AND e1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day e2 ON e2.base_dt = '{prev_days[3]}' AND e2.stock_cd = a.stock_cd AND e2.inv_div = '9000'
    LEFT JOIN stock.tb_inv_net_buy_day f1 ON f1.base_dt = '{prev_days[4]}' AND f1.stock_cd = a.stock_cd AND f1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day f2 ON f2.base_dt = '{prev_days[4]}' AND f2.stock_cd = a.stock_cd AND f2.inv_div = '9000'
    LEFT JOIN stock.tb_inv_net_buy_day g1 ON g1.base_dt = '{prev_days[5]}' AND g1.stock_cd = a.stock_cd AND g1.inv_div = '7050'
    LEFT JOIN stock.tb_inv_net_buy_day g2 ON g2.base_dt = '{prev_days[5]}' AND g2.stock_cd = a.stock_cd AND g2.inv_div = '9000'
    GROUP BY a.base_dt, a.stock_cd, a.stock_nm
"""

# INSERT 실행
with engine.begin() as conn:

    print(f"[i] {base_dt} 기준 새 데이터 생성 중...", insert_sql )
    conn.execute(text(insert_sql))

    print(f"[✓] {base_dt} 기준 데이터가 성공적으로 삭제 후 재삽입되었습니다.")


# 엑셀파일로 생성

# 저장 디렉토리
save_dir = r"D:\python_proj\venv_stock\stock_file"

# 파일 이름
excel_filename = f"tb_stock_inv_trx_m_{base_date}.xlsx"
file_path = os.path.join(save_dir, excel_filename)

# 데이터 조회
query = f"""
SELECT * 
FROM stock.tb_stock_inv_trx_m 
WHERE base_dt = '{base_date}'
"""

df = pd.read_sql(query, engine)

# 엑셀로 저장
#df.to_excel(file_path, index=False)

#print(f"[✔] 엑셀 파일 저장 완료: {file_path}")
