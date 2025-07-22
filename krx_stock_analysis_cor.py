
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, text
import os, sys

# MySQL 연결 설정
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"

def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"[!] Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False

# Argument에서 base_dt 받기 또는 사용자 입력 받기
if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
    base_dt = sys.argv[1]
else:
    while True:
        base_dt = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
        if validate_date(base_dt):
            break
        print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")

# SQLAlchemy 엔진 생성
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8"
)

with engine.connect() as conn:
    # 1. base_dt에 해당하는 work_seq 조회
    work_seq_query = text("""
        SELECT work_seq
        FROM stock.tb_work_day
        WHERE work_day = :base_dt
    """)
    result = conn.execute(work_seq_query, {'base_dt': base_dt}).fetchone()
    if result is None:
        raise ValueError(f"[!] No work_seq found for base_dt {base_dt}")
    work_seq = result[0]

    # 2. work_seq 기준 7일간의 work_day 추출
    seq_range_query = text("""
        SELECT work_day
        FROM stock.tb_work_day
        WHERE work_seq BETWEEN :start AND :end
        ORDER BY work_seq
    """)
    result = conn.execute(seq_range_query, {'start': work_seq, 'end': work_seq + 4})
    dates = [row[0] for row in result.fetchall()]

    if not dates:
        raise ValueError("[!] work_day 조회 실패")

    # 날짜 오름차순 정렬 (d1 ~ d7 순서를 위해)
    sorted_dates = sorted(dates)

    # 3. 가격정보 조회
    placeholders = ", ".join([f":date{i}" for i in range(len(sorted_dates))])
    price_query = text(f"""
        SELECT stock_cd, base_dt, price_gap_rate
        FROM stock.tb_stock_day_price
        WHERE base_dt IN ({placeholders})
    """)
    params = {f"date{i}": date for i, date in enumerate(sorted_dates)}
    price_df = pd.read_sql(price_query, conn, params=params)

    # 4. 중복 제거 및 Null 보정
    price_df_deduped = price_df.drop_duplicates(subset=["stock_cd", "base_dt"])

    # 각 종목별로 날짜가 빠진 경우 Null을 채움
    all_stock_cds = price_df_deduped["stock_cd"].unique()
    full_index = pd.MultiIndex.from_product([all_stock_cds, sorted_dates], names=["stock_cd", "base_dt"])
    price_df_complete = price_df_deduped.set_index(["stock_cd", "base_dt"]).reindex(full_index).reset_index()

    # 5. Pivot 처리 및 컬럼 재정의
    price_pivot = price_df_complete.pivot(index="stock_cd", columns="base_dt", values="price_gap_rate").reset_index()
    price_pivot = price_pivot[["stock_cd"] + sorted_dates]  # 날짜 정렬 유지
    price_pivot.columns = ['stock_cd'] + [f'd{i+1}_price' for i in range(len(sorted_dates))]

    # 6. 종목 정보 조회
    inv_query = text("""
        SELECT  
            base_dt, stock_cd, stock_nm, inst_cnt, inst_con_cnt,
            fore_cnt, fore_con_cnt, buy_con_cnt, avg_trx_qty,
            d1_trx_qty, d7_avg_trx_rate, buy_grade
        FROM stock.tb_stock_inv_trx_cnt 
        WHERE base_dt = :base_dt AND buy_grade in ( 'S', 'A', 'B' )
    """)
    inv_df = pd.read_sql(inv_query, conn, params={"base_dt": base_dt})

    # 7. price_pivot과 조인
    merged_df = pd.merge(inv_df, price_pivot, on="stock_cd", how="left")

    # 8. 엑셀 저장
    output_dir = r"D:\python_proj\venv_stock\stock_file"
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, f"stock_inv_trx_result_{base_dt}.xlsx")
    merged_df.to_excel(output_file, index=False)
    print(f"[✔] 저장 완료: {output_file}")

    # 9. 상관관계 분석
    if 'buy_con_cnt' in merged_df.columns and 'd2_price' in merged_df.columns:
        corr_1 = merged_df['buy_con_cnt'].corr(merged_df['d2_price'])
        print(f" 상관관계 (buy_con_cnt vs d2_price): {corr_1:.4f}")

    if 'd7_avg_trx_rate' in merged_df.columns and 'd2_price' in merged_df.columns:
        corr_2 = merged_df['d7_avg_trx_rate'].corr(merged_df['d2_price'])
        print(f" 상관관계 (d7_avg_trx_rate vs d2_price): {corr_2:.4f}")

