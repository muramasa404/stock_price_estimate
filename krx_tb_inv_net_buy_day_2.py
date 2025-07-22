

import requests
import pandas as pd
from datetime import datetime
import time
import io, sys
from sqlalchemy import create_engine, text

# MariaDB 연결 설정 (선택 사항)
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"
TABLE_NAME = "tb_inv_net_buy_day"

# SQLAlchemy 엔진 생성 (선택 사항)
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8"
)

# KRX 데이터 요청을 위한 헤더 설정
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020303",
}


# 투자자별 순매수 종목 데이터 요청 함수

def get_investor_net_buy_data(start_date, end_date, market, investor_type ):
    otp_url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    
    payload = {
        "locale" : "ko_KR",
        "mktId": market,  # ALL, STK: KOSPI, KSQ: KOSDAQ
        "strtDd": start_date,
        "endDd": end_date,
        "invstTpCd": investor_type,
        "csvxls_isNo": "false",
        "name": "fileDown",
        "share" : 1,
        "money" : 1,
        "url": "dbms/MDC/STAT/standard/MDCSTAT02401"
    }

    response = requests.post(otp_url, data=payload, headers=headers)
    print( response.text)

    if response.status_code != 200:
        print(f"Failed to get OTP: {response.status_code}")
        return None

    otp_code = response.text

    download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    csv_response = requests.post(download_url, data={"code": otp_code}, headers=headers)

    print( csv_response.text[:500]
          )
    # CSV 응답 내용 확인
    if csv_response.text.strip() == "":
        print("CSV response is empty.")
        return None

    if csv_response.status_code != 200:
        print(f"Failed to download CSV: {csv_response.status_code}")
        return None

    csv_response.encoding = "euc-kr"
    df = pd.read_csv(io.StringIO(csv_response.text), encoding="euc-kr")

    print(df.columns.tolist())

    return df


# 데이터를 테이블 칼럼에 매핑하는 함수 (선택 사항)
def map_to_table(df, date, investor_type):
    print(f"Mapping data for date: {date}")
    if df.empty:
        print("No data available in trading data")
        return None
    
    print("Available columns in CSV data:", df.columns.tolist())
    
    data_dict = {
        "BASE_DT": [date] * len(df),
        "INV_DIV": [investor_type] * len(df),
        "STOCK_CD": df["종목코드"],
        "STOCK_NM": df["종목명"],
        "TRADE_SEL_QTY": df["거래량_매도"],
        "TRADE_BUY_QTY": df["거래량_매수"],
        "TRADE_NET_BUY_QTY": df["거래량_순매수"],
        "TRADE_SEL_AMT": df["거래대금_매도"],
        "TRADE_BUY_AMT": df["거래대금_매수"],
        "TRADE_NET_BUY_AMT": df["거래대금_순매수"]
    }
    
    mapped_df = pd.DataFrame(data_dict)
    return mapped_df

# tb_inv_net_buy_day 에 데이터 삽입
def ins_tb_inv_net_buy_day (df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f"Data successfully inserted into {table_name}")
    except Exception as e:
        print(f"Error inserting data: {e}")

# tb_inv_net_buy_day 에 거래량_순매수 기준으로 rank 반영
def upd_tb_inv_net_buy_day_cnt( work_date, investor_type, engine):

    try:
        with engine.begin() as conn:

            # 기존 임시 테이블이 있으면 삭제
            drop_temp_sql = text("DROP TEMPORARY TABLE IF EXISTS tmp_rank_cnt;")
            conn.execute(drop_temp_sql)

            # 1단계: 중간 랭킹 결과를 임시 테이블에 저장
            create_temp_sql = text("""
                CREATE TEMPORARY TABLE tmp_rank_cnt AS
                SELECT 
                    BASE_DT,
                    INV_DIV,
                    STOCK_CD,
                    DENSE_RANK() OVER (PARTITION BY BASE_DT, INV_DIV ORDER BY TRADE_NET_BUY_QTY DESC) AS RNK_CNT
                FROM stock.tb_inv_net_buy_day
                WHERE BASE_DT = :base_dt
                AND INV_DIV = :inv_div
            """)
            conn.execute(create_temp_sql, {"base_dt": work_date, "inv_div": investor_type})

            # 2단계: 임시 테이블을 이용해 UPDATE 수행
            update_cnt_sql = text("""
                UPDATE stock.tb_inv_net_buy_day t
                JOIN tmp_rank_cnt r
                  ON t.BASE_DT = r.BASE_DT
                 AND t.INV_DIV = r.INV_DIV
                 AND t.STOCK_CD = r.STOCK_CD
                SET t.RANK_CNT = r.RNK_CNT
                WHERE t.BASE_DT = :base_dt
                AND t.INV_DIV = :inv_div
            """)
            conn.execute(update_cnt_sql, {"base_dt": work_date, "inv_div": investor_type})

            print(f"Updated RANK_CNT for BASE_DT = {work_date}, INV_DIV = {investor_type}")

    except Exception as e:
        print(f" Error update rank_cnt data: {e}")
        
   

# tb_inv_net_buy_day 에 거래대금_순매수 기준으로 rank 반영
def upd_tb_inv_net_buy_day_amt( work_date, investor_type, engine):

    try:
        with engine.begin() as conn:
            
            # 기존 임시 테이블이 있으면 삭제
            drop_temp_sql = text("DROP TEMPORARY TABLE IF EXISTS tmp_rank_amt;")
            conn.execute(drop_temp_sql)

            # 1단계: 중간 랭킹 결과를 임시 테이블에 저장
            create_temp_sql = text("""
                CREATE TEMPORARY TABLE tmp_rank_amt AS
                SELECT 
                    BASE_DT,
                    INV_DIV,
                    STOCK_CD,
                    DENSE_RANK() OVER (PARTITION BY BASE_DT, INV_DIV ORDER BY TRADE_NET_BUY_AMT DESC) AS RNK_AMT
                FROM stock.tb_inv_net_buy_day
                WHERE BASE_DT = :base_dt
                AND INV_DIV = :inv_div
            """)
            conn.execute(create_temp_sql, {"base_dt": work_date, "inv_div": investor_type})

            # 2단계: 임시 테이블을 이용해 UPDATE 수행
            update_amt_sql = text("""
                UPDATE stock.tb_inv_net_buy_day t
                JOIN tmp_rank_amt r
                  ON t.BASE_DT = r.BASE_DT
                 AND t.INV_DIV = r.INV_DIV
                 AND t.STOCK_CD = r.STOCK_CD
                SET t.RANK_AMT = r.RNK_AMT
                WHERE t.BASE_DT = :base_dt
                AND t.INV_DIV = :inv_div
            """)
            conn.execute(update_amt_sql, {"base_dt": work_date, "inv_div": investor_type})

            print(f"Updated RANK_AMT for BASE_DT = {work_date}, INV_DIV = {investor_type}")

    except Exception as e:
        print(f" Error update rank_amt data: {e}")

   

# tb_inv_net_buy_day 데이터 적재를 위해 초기화 
def del_tb_inv_net_buy_day( work_date, engine):

    # 실행할 SQL 정의
    del_sql = text ( 
    """
        DELETE 
        FROM stock.tb_inv_net_buy_day
        WHERE 1=1
        AND BASE_DT = :base_dt;
    """ 
    )

    # SQL 실행
    try:
        with engine.begin() as conn:
            conn.execute(del_sql, {"base_dt": work_date})
            print(f"Deleted records for BASE_DT = {work_date}")
    except Exception as e:
        print(f"Error delete tb_inv_net_buy_day: {e}")




# 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False
    
    
# 메인 실행
if __name__ == "__main__":

    
# Argument에서 base_dt 받기 또는 사용자 입력 받기
    if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
        work_date = sys.argv[1]
    else:
        while True:
            work_date = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
            if validate_date(work_date):
                break
            print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")


    # tb_inv_net_buy_day  초기화 처리 
    del_tb_inv_net_buy_day( work_date, engine)


    # 기관합계 investor_type="7050", 외국인 investor_type="9000"
    for investor_type in ["7050", "9000"]:
        print(f"\nFetching {investor_type} data...")
        
        start_date = work_date
        end_date = work_date 
        market = "ALL"

        print(f"Fetching investor net buy data for {start_date} to {end_date}...")
        net_buy_data = get_investor_net_buy_data(start_date, end_date, market, investor_type )
        
        if net_buy_data is not None:
            print("Raw Net Buy Data Preview:")
            print(net_buy_data.head())
            
            # 테이블에 저장하려면 아래 주석 해제
            
            mapped_data = map_to_table(net_buy_data, start_date, investor_type)
            if mapped_data is not None:
                print("Mapped Data Preview:")
                print(mapped_data.head())
                
                ins_tb_inv_net_buy_day(mapped_data, TABLE_NAME, engine)

                # 거래량_순매수 기준으로 rank 부여 
                upd_tb_inv_net_buy_day_cnt( work_date, investor_type, engine)

                # 거래대금금_순매수 기준으로 rank 부여 
                upd_tb_inv_net_buy_day_amt( work_date, investor_type, engine)

            else:
                print("Failed to map data due to empty net_buy_data")
            
        else:
            print("Failed to fetch investor net buy data")
        
        time.sleep(1)

    