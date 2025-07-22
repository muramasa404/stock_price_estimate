import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import numpy as np
import os
import sys
from datetime import datetime
import logging

# --- 1. 설정 변수 ---
# 설정 정보를 코드 상단에 모아두어 관리를 용이하게 합니다.
DB_URL = "mysql+mysqlconnector://root@localhost/stock?charset=utf8"
SAVE_DIR = r"D:\python_proj\venv_stock\stock_file"

# --- 2. 로깅 설정 ---
# print 대신 logging을 사용하여 로그를 체계적으로 관리합니다.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(message)s')

# --- 3. 유틸리티 함수 ---
def validate_date(date_str):
    """날짜 문자열이 YYYYMMDD 형식인지 검증합니다."""
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        return False

def count_max_con_buy(values):
    """연속된 양수(순매수)의 최대 길이를 계산합니다."""
    max_count = count = 0
    for v in values:
        if v > 0:
            count += 1
        else:
            max_count = max(max_count, count)
            count = 0
    max_count = max(max_count, count)  # 마지막까지 연속된 경우를 처리
    return max_count

# --- 4. 메인 실행 로직 ---
def main(base_date):
    """주어진 기준일자에 대한 투자자별 거래 데이터를 분석하고 저장합니다."""
    
    # DB 엔진 생성
    try:
        engine = create_engine(DB_URL)
    except ImportError:
        logging.error("mysql-connector-python 라이브러리를 찾을 수 없습니다. 'pip install mysql-connector-python'으로 설치해주세요.")
        sys.exit(1)

    # --- 데이터 조회 ---
    logging.info("데이터베이스에서 투자자별 거래내역 조회를 시작합니다.")
    # d1_trx_qty를 SQL에서 미리 계산하여 Python 연산 부하를 줄입니다.
    select_sql = text("""
        SELECT
            BASE_DT, STOCK_CD, STOCK_NM,
            D1A_TRADE_NET_BUY_QTY, D2A_TRADE_NET_BUY_QTY, D3A_TRADE_NET_BUY_QTY,
            D4A_TRADE_NET_BUY_QTY, D5A_TRADE_NET_BUY_QTY, D6A_TRADE_NET_BUY_QTY,
            D7A_TRADE_NET_BUY_QTY,
            D1B_TRADE_NET_BUY_QTY, D2B_TRADE_NET_BUY_QTY, D3B_TRADE_NET_BUY_QTY,
            D4B_TRADE_NET_BUY_QTY, D5B_TRADE_NET_BUY_QTY, D6B_TRADE_NET_BUY_QTY,
            D7B_TRADE_NET_BUY_QTY,
            (D1A_TRADE_NET_BUY_QTY + D1B_TRADE_NET_BUY_QTY) AS d1_trx_qty
        FROM stock.tb_stock_inv_trx_m
        WHERE BASE_DT = :base_date
    """)
    
    try:
        df = pd.read_sql(select_sql, engine, params={"base_date": base_date})
        if df.empty:
            logging.warning(f"{base_date} 기준일에 해당하는 데이터가 'tb_stock_inv_trx_m' 테이블에 없습니다.")
            return # 데이터가 없으면 함수 종료
        logging.info(f"{base_date} 기준 데이터 {len(df)}건을 성공적으로 조회했습니다.")
    except SQLAlchemyError as e:
        logging.error(f"데이터 조회 중 오류가 발생했습니다: {e}")
        sys.exit(1)


    # --- 지표 계산 ---
    logging.info("주요 지표 계산을 시작합니다.")
    
    # 컬럼 정의
    inst_cols = [f'D{i}A_TRADE_NET_BUY_QTY' for i in range(1, 8)]
    fore_cols = [f'D{i}B_TRADE_NET_BUY_QTY' for i in range(1, 8)]
    avg_cols = inst_cols + fore_cols
    buy_cols = ['D1A_TRADE_NET_BUY_QTY', 'D2A_TRADE_NET_BUY_QTY', 'D1B_TRADE_NET_BUY_QTY', 'D2B_TRADE_NET_BUY_QTY']

    # 순매수일수 및 연속 순매수일수 계산
    df['inst_cnt'] = df[inst_cols].gt(0).sum(axis=1)
    df['inst_con_cnt'] = df[inst_cols].apply(count_max_con_buy, axis=1)
    df['fore_cnt'] = df[fore_cols].gt(0).sum(axis=1)
    df['fore_con_cnt'] = df[fore_cols].apply(count_max_con_buy, axis=1)
    df['buy_con_cnt'] = df[buy_cols].gt(0).sum(axis=1) # D1, D2 기관/외인 동시 순매수일 수

    # [성능개선] apply 대신 벡터화 연산을 사용하여 평균 거래량 계산
    # 0 이하 값은 NaN으로 바꾼 뒤 평균을 계산하면 양수 값들의 평균만 남게 됩니다.
    df['avg_trx_qty'] = df[avg_cols].where(df[avg_cols] > 0).mean(axis=1).fillna(0)
    
    # WOW_QTY_RATE (d7_avg_trx_rate) 계산 (0으로 나누기 방지)
    df['d7_avg_trx_rate'] = np.where(
        df['avg_trx_qty'] > 0,
        (df['d1_trx_qty'] / df['avg_trx_qty']).round(1),
        0
    )
    logging.info("주요 지표 계산을 완료했습니다.")

    # --- DB 저장 (단일 트랜잭션 처리) ---
    insert_df = df[['BASE_DT', 'STOCK_CD', 'STOCK_NM',
                    'inst_cnt', 'inst_con_cnt', 'fore_cnt', 'fore_con_cnt',
                    'buy_con_cnt', 'avg_trx_qty', 'd1_trx_qty', 'd7_avg_trx_rate']]

    # [안정성개선] DELETE와 INSERT를 하나의 트랜잭션으로 묶어 데이터 정합성 보장
    try:
        with engine.begin() as conn:
            # 1. 기존 데이터 삭제
            del_sql = text("DELETE FROM stock.tb_stock_inv_trx_cnt WHERE BASE_DT = :base_dt")
            logging.info(f"{base_date}의 기존 데이터를 'tb_stock_inv_trx_cnt' 테이블에서 삭제합니다.")
            conn.execute(del_sql, {'base_dt': base_date})

            # 2. 신규 데이터 삽입
            logging.info(f"{len(insert_df)}건의 신규 데이터를 테이블에 저장합니다.")
            insert_df.to_sql('tb_stock_inv_trx_cnt', conn, schema='stock', if_exists='append', index=False)
            logging.info("데이터베이스 저장이 성공적으로 완료되었습니다.")
    except SQLAlchemyError as e:
        logging.error(f"데이터베이스 처리 중 오류가 발생하여 작업이 롤백되었습니다: {e}")
        sys.exit(1)


    # --- Rank 및 Grade 업데이트 (통합 쿼리) ---
    # [성능개선] 여러 UPDATE 쿼리를 JOIN을 사용한 단일 쿼리로 통합
    logging.info("순매수 순위, 등락률, 매수 등급 업데이트를 시작합니다.")
    update_sql = text("""
        UPDATE
            stock.TB_STOCK_INV_TRX_CNT T1
        LEFT JOIN (
            SELECT
                BASE_DT, STOCK_CD,
                SUM(CASE WHEN INV_DIV = '7050' THEN RANK_AMT ELSE 0 END) AS INV_RANK_AMT,
                SUM(CASE WHEN INV_DIV = '9000' THEN RANK_AMT ELSE 0 END) AS FOR_RANK_AMT
            FROM stock.TB_INV_NET_BUY_DAY
            WHERE BASE_DT = :base_date
            GROUP BY BASE_DT, STOCK_CD
        ) T2 ON T1.BASE_DT = T2.BASE_DT AND T1.STOCK_CD = T2.STOCK_CD
        LEFT JOIN stock.TB_STOCK_DAY_PRICE T3 ON T1.BASE_DT = T3.BASE_DT AND T1.STOCK_CD = T3.STOCK_CD
        SET
            T1.INV_RANK_AMT = COALESCE(T2.INV_RANK_AMT, 0),
            T1.FOR_RANK_AMT = COALESCE(T2.FOR_RANK_AMT, 0),
            T1.PRICE_GAP_RATE = T3.PRICE_GAP_RATE,
            T1.BUY_GRADE = CASE
                WHEN (T1.BUY_CON_CNT >= 3 AND T1.D7_AVG_TRX_RATE > 1.3) OR T1.D7_AVG_TRX_RATE >= 2 THEN 'S'
                WHEN T1.INST_CNT + T1.FORE_CNT >= 10 THEN 'A'
                ELSE 'B'
            END
        WHERE
            T1.BASE_DT = :base_date;
    """)
    try:
        with engine.begin() as conn:
            conn.execute(update_sql, {'base_date': base_date})
        logging.info("순위, 등락률, 등급 컬럼 업데이트가 성공적으로 완료되었습니다.")
    except SQLAlchemyError as e:
        logging.error(f"Rank 및 Grade 업데이트 중 DB 오류가 발생했습니다: {e}")
        sys.exit(1)


    # --- 엑셀 파일 저장 ---
    logging.info("최종 결과 데이터 조회 및 엑셀 파일 저장을 시작합니다.")
    os.makedirs(SAVE_DIR, exist_ok=True)
    
    # 1. 최종 결과 저장
    result_sql = text("""
        SELECT
             A.*, B.RSI, B.OBV
        FROM
             stock.TB_STOCK_INV_TRX_CNT A
             JOIN stock.TB_STOCK_TRX_IDX B ON A.BASE_DT = B.BASE_DT AND A.STOCK_CD = B.STOCK_CD
        WHERE A.BASE_DT = :base_date
        ORDER BY A.BUY_GRADE, A.STOCK_NM
    """)
    result_df = pd.read_sql(result_sql, engine, params={"base_date": base_date})
    excel_filename = f"tb_stock_inv_trx_cnt_{base_date}.xlsx"
    file_path = os.path.join(SAVE_DIR, excel_filename)
    result_df.to_excel(file_path, index=False)
    logging.info(f"분석 결과 엑셀 파일 저장 완료: {file_path}")

    # 2. 통계 요약 저장
    stock_cnt_sql = text("""
        SELECT stock_cd, stock_nm, COUNT(1) AS record_count
        FROM stock.tb_stock_inv_trx_m
        WHERE base_dt <= :base_date
        GROUP BY stock_cd, stock_nm
        ORDER BY record_count DESC
    """)
    stock_cnt_df = pd.read_sql(stock_cnt_sql, engine, params={"base_date": base_date})
    stock_cnt_filename = f"stock_trx_analysis_{base_date}.xlsx"
    stock_cnt_file_path = os.path.join(SAVE_DIR, stock_cnt_filename)
    stock_cnt_df.to_excel(stock_cnt_file_path, index=False)
    logging.info(f"종목별 데이터 수집 빈도 엑셀 파일 저장 완료: {stock_cnt_file_path}")


if __name__ == "__main__":
    # Argument 또는 사용자 입력을 통해 기준일자 받기
    if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
        base_dt = sys.argv[1]
    else:
        if len(sys.argv) >= 2:
            print(f"입력한 날짜 '{sys.argv[1]}'가 잘못된 형식입니다. YYYYMMDD 형식으로 입력해주세요.")
        
        while True:
            base_dt = input("기준일자를 입력하세요 (YYYYMMDD, 예: 20250404): ").strip()
            if validate_date(base_dt):
                break
            print("잘못된 날짜 형식입니다. 다시 입력해주세요.")

    logging.info(f"====== {base_dt} 기준 데이터 처리 시작 ======")
    main(base_dt)
    logging.info(f"====== {base_dt} 기준 데이터 처리 완료 ======")