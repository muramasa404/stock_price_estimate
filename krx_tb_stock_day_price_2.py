import requests
import pandas as pd
from datetime import datetime
import time
import io
import sys, os
import mysql.connector
from sqlalchemy import create_engine, text


# MySQL 연결 설정
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"
TABLE_NAME = "tb_stock_day_price"

# SQLAlchemy 엔진 생성
engine = create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8")

# KRX 요청 헤더
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Referer": "http://data.krx.co.kr/contents/MDC/MDI/mdiLoader/index.cmd?menuId=MDC0201020101",
}

def get_krx_stock_data(date, market="STK"):
    url = "http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd"
    payload = {
        "mktId": market,
        "trdDd": date,
        "share": "1",
        "money": "1",
        "csvxls_isNo": "false",
        "name": "fileDown",
        "url": "dbms/MDC/STAT/standard/MDCSTAT01501"
    }
    response = requests.post(url, data=payload, headers=headers)
    if response.status_code != 200:
        print(f"Failed to get OTP: {response.status_code}")
        return None
    otp_code = response.text
    download_url = "http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd"
    csv_response = requests.post(download_url, data={"code": otp_code}, headers=headers)
    if csv_response.status_code != 200:
        print(f"Failed to download CSV: {csv_response.status_code}")
        return None
    csv_response.encoding = "euc-kr"
    return pd.read_csv(io.StringIO(csv_response.text), encoding="euc-kr")

def map_to_table(df, date, market):
    if df.empty:
        print("No data available in dataframe")
        return None
    data_dict = {
        "BASE_DT": [date] * len(df),
        "STOCK_CD": df["종목코드"],
        "STOCK_NM": df["종목명"],
        "MRKT_DIV": [market] * len(df),
        "STOCK_MNG": [None] * len(df),
        "CLOSE_PRICE": df["종가"],
        "PRICE_GAP": df["대비"],
        "PRICE_GAP_RATE": df["등락률"],
        "OPEN_PRICE": df["시가"],
        "HIGH_PRICE": df["고가"],
        "LOW_PRICE": df["저가"],
        "TRADE_QTY": df["거래량"],
        "TRADE_AMT": df["거래대금"],
        "MRKT_CAPITAL": df["시가총액"],
        "ISSUE_STOCK_QTY": df["상장주식수"]
    }
    return pd.DataFrame(data_dict)

def insert_to_mysql(df, table_name, engine):
    try:
        df.to_sql(table_name, con=engine, if_exists="append", index=False)
        print(f" Data inserted into {table_name}")
    except Exception as e:
        print(f" Error inserting data: {e}")

def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"[!] Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False

if __name__ == "__main__":

    save_dir = r"D:\python_proj\venv_stock\stock_file"
    os.makedirs(save_dir, exist_ok=True)



# 🔹 Argument에서 base_dt 받기 또는 사용자 입력 받기
    if len(sys.argv) >= 2 and validate_date(sys.argv[1]):
        work_date = sys.argv[1]
    else:
        while True:
            work_date = input("Enter base date (YYYYMMDD, e.g., 20250404): ").strip()
            if validate_date(work_date):
                break
            print("잘못된 날짜 형식입니다. 다시 입력해주세요 (예: 20250404)")

    ## tb_stock_day_price 테이블에 데이터 삭제
    del_d_sql = text("DELETE FROM stock.tb_stock_day_price WHERE BASE_DT = :work_date")

    with engine.begin() as conn:

        print(f"[i] {work_date} tb_stock_day_price 데이터 삭제 중...", del_d_sql)
        conn.execute(del_d_sql, {'work_date': work_date})
        print(f"[✔] {work_date} tb_stock_day_price 데이터 삭제 완료")

    #  tb_stock_day_price 테이블에 데이터 삽입, 엑셀 파일 생성

    for market, market_name in [("STK", "KOSPI"), ("KSQ", "KOSDAQ")]:
        print(f"\n {market_name} 데이터 다운로드 중: {work_date}")
        stock_data = get_krx_stock_data(work_date, market=market)

        if stock_data is not None:
            print(stock_data.head())
            excel_filename = f"{market_name.lower()}_stock_data_{work_date}.xlsx"
            file_path = os.path.join(save_dir, excel_filename)
            #stock_data.to_excel(file_path, index=False, engine="openpyxl")

            print(f" 저장됨: {file_path}")
            mapped = map_to_table(stock_data, work_date, market_name)

            if mapped is not None:
                print(mapped.head())
                insert_to_mysql(mapped, TABLE_NAME, engine)
        else:
            print(f" {market_name} 데이터를 가져오는 데 실패했습니다.")

        time.sleep(1)