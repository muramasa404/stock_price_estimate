

import pandas as pd
import pymysql
from sqlalchemy import create_engine
from datetime import datetime
import sys

from sqlalchemy import text

# ✅ DB 연결 함수
def get_db_connection():
    return create_engine("mysql+pymysql://root@localhost:3306/stock")  # DB 정보 수정 필요

# ✅ 날짜 유효성 검사
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"[!] Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False

# ✅ RSI 계산 함수
def calculate_rsi(df, period: int = 14):
    df = df.sort_values('BASE_DT')
    delta = df['CLOSE_PRICE'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))

    df['RSI'] = rsi
    return df



# ✅ OBV 계산 함수
def calculate_obv(df):
    df = df.sort_values('BASE_DT').reset_index(drop=True)  # 인덱스 재설정 중요!
    obv = [0]
    for i in range(1, len(df)):
        if df.iloc[i]['CLOSE_PRICE'] > df.iloc[i - 1]['CLOSE_PRICE']:
            obv.append(obv[-1] + df.iloc[i]['TRADE_QTY'])
        elif df.iloc[i]['CLOSE_PRICE'] < df.iloc[i - 1]['CLOSE_PRICE']:
            obv.append(obv[-1] - df.iloc[i]['TRADE_QTY'])
        else:
            obv.append(obv[-1])
    df['OBV'] = obv
    return df


# ✅ RSI & OBV 계산 및 저장 함수
def compute_and_insert_indicators(base_date: str):
    engine = get_db_connection()
    conn = engine.raw_connection()
    cursor = conn.cursor()

    # ✅ 기준일의 work_seq 값 가져오기
    query_seq = f"""
        SELECT work_seq FROM stock.tb_work_day
        WHERE work_day = '{base_date}'
    """
    with engine.connect() as con:
        result = con.execute(text(query_seq)).fetchone()
        if result is None:
            raise ValueError("기준일의 WORK_SEQ 값을 찾을 수 없습니다.")
        base_seq = result[0]

    # ✅ 기준일 포함, 과거 14일의 WORK_DAY 가져오기
    query_days = f"""
        SELECT work_day FROM stock.tb_work_day
        WHERE work_seq BETWEEN {base_seq - 14} AND {base_seq}
        ORDER BY work_seq
    """
    with engine.connect() as con:
        days = [row[0] for row in con.execute(text(query_days)).fetchall()]
        if len(days) < 15:
            raise ValueError("거래일 수가 부족합니다.")

    start_date = days[0]   # 과거 14일 전
    end_date = days[-1]    # 기준일

    print(f"📅 기준일: {end_date}, 조회 시작일: {start_date}")

    # ✅ 데이터 조회
    query = f"""
        SELECT BASE_DT, STOCK_CD, STOCK_NM, CLOSE_PRICE, TRADE_QTY
        FROM tb_stock_day_price
        WHERE BASE_DT BETWEEN '{start_date}' AND '{end_date}'
    """
    df = pd.read_sql(query, engine)

    # ✅ 종목별 RSI + OBV 계산
    def process_group(group_df):
        group_df = calculate_rsi(group_df)
        group_df = calculate_obv(group_df)
        return group_df

    result_df = (
        df.groupby('STOCK_CD', group_keys=False)
          .apply(process_group)
          .dropna(subset=['RSI'])
          .reset_index(drop=True)
    )

    print(result_df.head())  # 디버깅용 출력

    # ✅ INSERT 또는 UPSERT
    insert_sql = """
        INSERT INTO tb_stock_trx_idx (BASE_DT, STOCK_CD, STOCK_NM, CLOSE_PRICE, RSI, OBV)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            CLOSE_PRICE = VALUES(CLOSE_PRICE),
            RSI = VALUES(RSI),
            OBV = VALUES(OBV)
    """

    for _, row in result_df.iterrows():
        cursor.execute(insert_sql, (
            row['BASE_DT'],
            row['STOCK_CD'],
            row['STOCK_NM'],
            row['CLOSE_PRICE'],
            float(row['RSI']),
            float(row['OBV'])
        ))
        print(f"✅ Inserted/Updated: {row['STOCK_CD']} on {row['BASE_DT']}")

    conn.commit()
    cursor.close()
    conn.close()
    print(f"✅ RSI & OBV 저장 완료 ({start_date} ~ {end_date})")

    


# ✅ 메인 실행부

if __name__ == "__main__":
    # 🔹 시작일자 처리
    if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
        start_date = sys.argv[1]
    else:
        while True:
            start_date = input("Enter START date (YYYYMMDD): ").strip()
            if validate_date(start_date):
                break
            print("[!] Invalid date format. Please try again.")

    # 🔹 종료일자 처리: 인자가 없거나 유효하지 않으면 시작일자로 대체
    if len(sys.argv) >= 3 and validate_date(sys.argv[2]):
        end_date = sys.argv[2]
    else:
        end_date = start_date
        print(f"[ℹ] END date not provided or invalid. Using START date ({start_date}) instead.")

    compute_and_insert_indicators(base_date=end_date)

    print(f"[✔] RSI & OBV indicators computed and inserted for {start_date} to {end_date}.")