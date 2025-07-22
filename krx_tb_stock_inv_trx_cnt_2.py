

import pandas as pd
from sqlalchemy import create_engine, text
import time
from datetime import datetime
import os, sys
import re
from sqlalchemy.exc import SQLAlchemyError

import numpy as np  # numpy 추가



# 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False
    

#  Argument에서 base_dt 받기 또는 사용자 입력 받기
if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
    base_date = sys.argv[1]
else:
    while True:
        base_date = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
        if validate_date(base_date):
            break
        print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")



# DB 연결
engine = create_engine("mysql+mysqlconnector://root@localhost/stock?charset=utf8")

# 기 입력되어 있는 데이터 삭제
del_c_sql = text("DELETE FROM stock.tb_stock_inv_trx_cnt WHERE BASE_DT = :base_dt")

# 실행
try :
    with engine.begin() as conn:
        print(f"[i] {base_date} tb_stock_inv_trx_cnt 데이터 삭제 중...", del_c_sql)
        conn.execute(del_c_sql, {'base_dt': base_date})
        time.sleep(1)
        print(f"[✔] {base_date} tb_stock_inv_trx_cnt 데이터 삭제 완료")

except SQLAlchemyError as e:
    print(" DB 삭제 중 오류가 발생했습니다.")
    print(f"에러 내용: {e}")
    sys.exit(1)


# tb_stock_inv_trx_m  데이터 조회
select_sql = text("""
SELECT 
    BASE_DT,
    STOCK_CD,
    STOCK_NM,
    D1A_TRADE_NET_BUY_QTY, D2A_TRADE_NET_BUY_QTY, D3A_TRADE_NET_BUY_QTY,
    D4A_TRADE_NET_BUY_QTY, D5A_TRADE_NET_BUY_QTY, D6A_TRADE_NET_BUY_QTY, 
    D7A_TRADE_NET_BUY_QTY,
    D1B_TRADE_NET_BUY_QTY, D2B_TRADE_NET_BUY_QTY, D3B_TRADE_NET_BUY_QTY,
    D4B_TRADE_NET_BUY_QTY, D5B_TRADE_NET_BUY_QTY, D6B_TRADE_NET_BUY_QTY, 
    D7B_TRADE_NET_BUY_QTY
FROM stock.tb_stock_inv_trx_m
WHERE 1=1
#AND STOCK_CD = '000150'
AND BASE_DT = :base_date
""")

df = pd.read_sql(select_sql, engine, params={"base_date": base_date})
print(" tb_stock_inv_trxm_m ", df)

# 최대 연속 양수 카운팅 함수
def count_max_con_buy(values):
    max_count = count = 0
    for v in values:
        if v > 0:
            count += 1
            max_count = max(max_count, count)
        else:
            count = 0
    return max_count

# 컬럼 분류
inst_cols = [f'D{i}A_TRADE_NET_BUY_QTY' for i in range(1, 8)]
fore_cols = [f'D{i}B_TRADE_NET_BUY_QTY' for i in range(1, 8)]
buy_cols = ['D1A_TRADE_NET_BUY_QTY', 'D2A_TRADE_NET_BUY_QTY', 'D1B_TRADE_NET_BUY_QTY', 'D2B_TRADE_NET_BUY_QTY']
avg_cols = inst_cols + fore_cols

# 컬럼별 연산
df['inst_cnt'] = df[inst_cols].gt(0).sum(axis=1)
print("inst_cnt", df['inst_cnt'])

df['inst_con_cnt'] = df[inst_cols].apply(count_max_con_buy, axis=1)
print("inst_con_cnt", df['inst_con_cnt'])

df['fore_cnt'] = df[fore_cols].gt(0).sum(axis=1)
print("fore_cnt", df['fore_cnt'])

df['fore_con_cnt'] = df[fore_cols].apply(count_max_con_buy, axis=1)
print("fore_con_cnt", df['fore_con_cnt'])

df['buy_con_cnt'] = df[buy_cols].apply(count_max_con_buy, axis=1)
print("buy_con_cnt", df['buy_con_cnt'])


# D1 거래량 합계
df['d1_trx_qty'] = df['D1A_TRADE_NET_BUY_QTY'] + df['D1B_TRADE_NET_BUY_QTY']
print("d1_trx_qty", df['d1_trx_qty'])

# 평균 거래량 (0 또는 NaN 제외)
df['avg_trx_qty'] = df[avg_cols].apply(lambda row: row[row > 0].mean(), axis=1)
print("avg_trx_qty", df['avg_trx_qty'])

# WOW_QTY_RATE 계산 (0 또는 NaN 방지)
df['d7_avg_trx_rate'] = np.where(
    # 평균이 0 이하인 경우 trad_volume을 0으로 처리
    df['avg_trx_qty'] > 0,
    (df['d1_trx_qty'] / df['avg_trx_qty']).round(1), 0
)
print("d7_avg_trx_rate", df['d7_avg_trx_rate'])


# 최종 저장할 컬럼
insert_df = df[['BASE_DT', 'STOCK_CD', 'STOCK_NM',
                'inst_cnt', 'inst_con_cnt',
                'fore_cnt', 'fore_con_cnt',
                'buy_con_cnt', 'avg_trx_qty', 'd1_trx_qty', 'd7_avg_trx_rate']]

# DB 저장
try:
    insert_df.to_sql('tb_stock_inv_trx_cnt', engine, schema='stock', if_exists='append', index=False)
    print(" DB에 성공적으로 저장되었습니다.")
except SQLAlchemyError as e:
    print(" DB 저장 중 오류가 발생했습니다.")
    print(f"에러 내용: {e}")


# grade 계산 함수 정의
def calculate_grade(row):
    if row['buy_con_cnt'] >= 3 or row.get('D7_AVG_TRX_RATE', 0) >= 2:
        return 'S'
    elif (row['inst_cnt'] + row['fore_cnt']) >= 10:
        return 'A'
    else:
        return 'B'

# grade 컬럼 추가
df['buy_grade'] = df.apply(calculate_grade, axis=1)

# 기관, 외인 buy rank 업데이트 
update_rank_sql = """
UPDATE STOCK.TB_STOCK_INV_TRX_CNT T1
SET 
    T1.INV_RANK_AMT = (
        SELECT SUM(CASE WHEN INV_DIV = '7050' THEN RANK_AMT ELSE NULL END)
        FROM STOCK.TB_INV_NET_BUY_DAY T2
        WHERE T2.BASE_DT = T1.BASE_DT
        AND T2.STOCK_CD = T1.STOCK_CD
        GROUP BY T2.BASE_DT, T2.STOCK_CD
    ),
    T1.FOR_RANK_AMT = (
        SELECT SUM(CASE WHEN INV_DIV = '9000' THEN RANK_AMT ELSE NULL END)
        FROM STOCK.TB_INV_NET_BUY_DAY T2
        WHERE T2.BASE_DT = T1.BASE_DT
        AND T2.STOCK_CD = T1.STOCK_CD
        GROUP BY T2.BASE_DT, T2.STOCK_CD
    ),
    T1.PRICE_GAP_RATE = (
        SELECT PRICE_GAP_RATE
        FROM STOCK.TB_STOCK_DAY_PRICE T3
        WHERE T3.BASE_DT = T1.BASE_DT
        AND T3.STOCK_CD = T1.STOCK_CD
    )
WHERE T1.BASE_DT = :base_date

"""

with engine.begin() as conn:
    conn.execute(text(update_rank_sql), {'base_date': base_date})
    print(f"[✔] rank 컬럼이 업데이트되었습니다.")


# 기관, 외인 buy rank 업데이트 

update_grade_sql = """
UPDATE STOCK.TB_STOCK_INV_TRX_CNT T1
SET T1.BUY_GRADE = CASE
        WHEN (BUY_CON_CNT >= 3 AND D7_AVG_TRX_RATE > 1.3) OR D7_AVG_TRX_RATE >= 2 THEN 'S'
        WHEN INST_CNT + FORE_CNT >= 10 THEN 'A'
        ELSE 'B' END

WHERE T1.BASE_DT = :base_date       

"""

with engine.begin() as conn:
    conn.execute(text(update_grade_sql), {'base_date': base_date})
    print(f"[✔] grade 컬럼이 업데이트되었습니다.")



# 엑셀 저장
save_dir = r"D:\python_proj\venv_stock\stock_file"
os.makedirs(save_dir, exist_ok=True)

excel_filename = f"tb_stock_inv_trx_cnt_{base_date}.xlsx"
file_path = os.path.join(save_dir, excel_filename)

# 저장된 결과 재조회 및 저장
result_sql = text(""" 
    SELECT 
         A.*
        ,B.RSI
        ,B.OBV
    FROM 
         STOCK.TB_STOCK_INV_TRX_CNT A 
        ,STOCK.TB_STOCK_TRX_IDX  B  
    WHERE 1=1
    AND A.BASE_DT = :base_date
    AND B.BASE_DT = A.BASE_DT
    AND B.STOCK_CD = A.STOCK_CD
                             
    ORDER BY A.STOCK_CD """)

result_df = pd.read_sql(result_sql, engine, params={"base_date": base_date})
print(" tb_stock_inv_trx_cnt ", result_df)

result_df.to_excel(file_path, index=False)

print(f"[✔] 엑셀 파일 저장 완료: {file_path}")


# 통계 요약 SQL 실행 및 엑셀 저장

stock_cnt_sql = text("""
SELECT stock_cd, stock_nm, COUNT(1) AS record_count
FROM stock.tb_stock_inv_trx_m
WHERE base_dt <= :base_date
GROUP BY stock_cd, stock_nm
ORDER BY record_count DESC
""")

stock_cnt_df = pd.read_sql(stock_cnt_sql, engine, params={"base_date": base_date})
print(" tb_stock_inv_trx_m stock_cd 빈도수 ", stock_cnt_df)

stock_cnt_filename = f"stock_trx_analysis_{base_date}.xlsx"
stock_cnt_file_path = os.path.join(save_dir, stock_cnt_filename)

stock_cnt_df.to_excel( stock_cnt_file_path, index=False)
print(f"[✔] 요약 엑셀 파일 저장 완료: {stock_cnt_file_path}")
