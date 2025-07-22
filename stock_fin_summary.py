"""
https://github.com/FinanceData/FinanceDataReader?tab=readme-ov-file

pip install --upgrade finance-datareader pymysql sqlalchemy


OPMARGIN       FLOAT,     -- 영업이익률 (%)
NETMARGIN      FLOAT,     -- 순이익률 (%)
ROE            FLOAT,     -- 자기자본이익률 (%)
ROA            FLOAT,     -- 총자산이익률 (%)
DEBT_RATIO     FLOAT,     -- 부채비율 (%)
EPS            FLOAT,     -- 주당순이익 (원)
PER            FLOAT,     -- PER (배)
BPS            FLOAT,     -- 주당순자산 (원)
PBR            FLOAT,     -- 주가 / BPS (계산 필요)
PEG            FLOAT,     -- PER / EPS 증가율 (추정 또는 생략)
MARKET_CAP     BIGINT,    -- 시가총액 (옵션)

현재주가 (FinanceDataReader.DataReader 사용)

시가총액 = 현재주가 × 보통주 발행주식수

EPS 증감률 = (EPS2024 - EPS2023) / EPS2023 × 100

PEG = PER / EPS 증가율 (정상값일 때만 계산)



 

"""

import pandas as pd
import FinanceDataReader as fdr
from sqlalchemy import create_engine
import pymysql
import os
from datetime import datetime
import logging
import time
import random
import requests.exceptions

# --- MariaDB 연결 설정 ---
MYSQL_USER = "root"
MYSQL_PASSWORD = "" 
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"
FIN_SUMMARY_TABLE_NAME = "tb_stock_fin_summary"

# SQLAlchemy 엔진 생성 
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8"
)

# --- 저장 경로 및 날짜 ---
EXPORT_FOLDER = r'D:\python_proj\venv_stock\stock_file'
EXPORT_DATE = datetime.now().strftime('%Y%m%d')
# EXPORT_FILE will now be dynamic based on the input year

# --- Logging 설정 ---
logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[
                        logging.FileHandler(os.path.join(EXPORT_FOLDER, f"stock_fin_summary_{EXPORT_DATE}.log")),
                        logging.StreamHandler()
                    ])

# --- Configuration for data fetching ---
MAX_RETRIES = 3
RETRY_DELAY_BASE = 5 # seconds
REQUEST_TIMEOUT = 15 # seconds for the API call itself

# --- 종목 코드 목록 조회 ---
def fetch_stock_codes():
    query = "SELECT STOCK_CD FROM stock.tb_stock_code"
    try:
        df = pd.read_sql(query, con=engine)
        return df['STOCK_CD'].tolist()
    except Exception as e:
        logging.error(f"종목 코드 조회 중 오류 발생: {e}")
        return []

# --- 재무 데이터 수집 (단일 연도용으로 수정) ---
def extract_fin_summary(stock_cd, target_year):
    df = None 
    for attempt in range(MAX_RETRIES):
        try:
            df = fdr.SnapDataReader(f'NAVER/FINSTATE/{stock_cd}')
            
            if df.empty:
                logging.warning(f"[{stock_cd}] SnapDataReader 결과 없음, 데이터 건너뜜 (시도 {attempt+1}/{MAX_RETRIES})")
                return None

            df.index = pd.to_datetime(df.index)
            df['연도'] = df.index.year

            # Filter for the target year only
            df_target_year = df[df['연도'] == target_year]

            if df_target_year.empty:
                logging.warning(f"[{stock_cd}] {target_year}년 재무 데이터 부족, 데이터 건너뜜 (시도 {attempt+1}/{MAX_RETRIES})")
                return None
            
            # Get the first row for the target year (assuming one entry per year per company)
            row_target_year = df_target_year.iloc[0]

            base_year_str = row_target_year.name.strftime('%Y%m%d')

            def safe_float(val):
                try:
                    if pd.isna(val):
                        return None
                    return float(val)
                except (ValueError, TypeError):
                    return None

            def safe_int(val):
                try:
                    if val is None or pd.isna(val):
                        return None
                    # Attempt to convert to float first to handle string representations of numbers like '123.0'
                    # Then convert to int if it's a whole number.
                    float_val = float(val)
                    if float_val.is_integer():
                        return int(float_val)
                    return None # Return None if it's a float that's not an integer (e.g., 123.45)
                except (ValueError, TypeError):
                    return None

            # Extract values for the target year, including all specified columns
            return {
                'STOCK_CD': stock_cd,
                'BASE_YEAR': base_year_str, 
                'SALES': safe_int(row_target_year.get('매출액')),
                'OP_PROFIT': safe_int(row_target_year.get('영업이익')),
                'OP_PROFIT_ANNOUNCED': safe_int(row_target_year.get('영업이익(발표기준)')), # Added
                'PRETAX_CONT_PROFIT': safe_int(row_target_year.get('세전계속사업이익')), # Added
                'NET_PROFIT': safe_int(row_target_year.get('당기순이익')),
                'NET_PROFIT_GOVERNED': safe_int(row_target_year.get('당기순이익(지배)')), # Added
                'NET_PROFIT_NON_GOVERNED': safe_int(row_target_year.get('당기순이익(비지배)')), # Added
                'TOTAL_ASSETS': safe_int(row_target_year.get('자산총계')),
                'TOTAL_DEBT': safe_int(row_target_year.get('부채총계')),
                'TOTAL_EQUITY': safe_int(row_target_year.get('자본총계')),
                'TOTAL_EQUITY_GOVERNED': safe_int(row_target_year.get('자본총계(지배)')), # Added
                'TOTAL_EQUITY_NON_GOVERNED': safe_int(row_target_year.get('자본총계(비지배)')), # Added
                'CAPITAL_STOCK': safe_int(row_target_year.get('자본금')), # Added
                'CASH_FLOW_OPERATING': safe_int(row_target_year.get('영업활동현금흐름')), # Added
                'CASH_FLOW_INVESTING': safe_int(row_target_year.get('투자활동현금흐름')), # Added
                'CASH_FLOW_FINANCING': safe_int(row_target_year.get('재무활동현금흐름')), # Added
                'CAPEX': safe_int(row_target_year.get('CAPEX')), # Added
                'FCF': safe_int(row_target_year.get('FCF')), # Added
                'INTEREST_BEARING_DEBT': safe_int(row_target_year.get('이자발생부채')), # Added
                'OPMARGIN': safe_float(row_target_year.get('영업이익률')),
                'NETMARGIN': safe_float(row_target_year.get('순이익률')),
                'ROE': safe_float(row_target_year.get('ROE(%)')),
                'ROA': safe_float(row_target_year.get('ROA(%)')),
                'DEBT_RATIO': safe_float(row_target_year.get('부채비율')),
                'RETAINED_EARNINGS_RATIO': safe_float(row_target_year.get('자본유보율')), # Added
                'EPS': safe_float(row_target_year.get('EPS(원)')),
                'EPS_GROWTH': None, # Not calculated for a single year
                'PER': safe_float(row_target_year.get('PER(배)')),
                'BPS': safe_float(row_target_year.get('BPS(원)')),
                'PBR': safe_float(row_target_year.get('PBR(배)')), 
                'CASH_DPS': safe_int(row_target_year.get('현금DPS(원)')), # Added
                'CASH_DIV_YIELD': safe_float(row_target_year.get('현금배당수익률')), # Added
                'CASH_DIV_PAYOUT_RATIO': safe_float(row_target_year.get('현금배당성향(%)')), # Added
                'ISSUED_SHARES_COMMON': safe_int(row_target_year.get('발행주식수(보통주)')), # Added
                'PEG': None      # Explicitly excluded
            }

        except (requests.exceptions.ConnectTimeout, requests.exceptions.RequestException) as e:
            logging.warning(f"[{stock_cd}] 네트워크/연결 오류 발생 (시도 {attempt+1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                sleep_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"[{stock_cd}] {int(sleep_time)}초 후 재시도...")
                time.sleep(sleep_time)
            else:
                logging.error(f"[{stock_cd}] 최대 재시도 횟수 ({MAX_RETRIES}) 도달. 재무 데이터 추출 실패: {e}")
                if df is not None:
                    logging.debug(f"[{stock_cd}] 오류 시점의 데이터 일부:\n{df.head()}")
                return None
        except IndexError as e:
            logging.error(f"[{stock_cd}] FinanceDataReader 내부 오류 (IndexError: list index out of range) 발생 (시도 {attempt+1}/{MAX_RETRIES}): {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1:
                sleep_time = RETRY_DELAY_BASE * (2 ** attempt) + random.uniform(0, 1)
                logging.info(f"[{stock_cd}] {int(sleep_time)}초 후 재시도...")
                time.sleep(sleep_time)
            else:
                logging.error(f"[{stock_cd}] 최대 재시도 횟수 ({MAX_RETRIES}) 도달. 재무 데이터 추출 실패.")
                return None
        except Exception as e:
            logging.error(f"[{stock_cd}] 예상치 못한 오류 발생 (시도 {attempt+1}/{MAX_RETRIES}): {e}", exc_info=True)
            if df is not None:
                logging.debug(f"[{stock_cd}] 오류 시점의 데이터 일부:\n{df.head()}")
            return None
    return None 

# --- INSERT ... ON DUPLICATE KEY UPDATE로 중복 업데이트 저장 ---
def insert_fin_summary_on_duplicate_key_update(data_list):
    if not data_list:
        logging.info("저장할 데이터 없음")
        return

    # Assuming the database table 'tb_stock_fin_summary' has columns for all new fields.
    # If not, you'll need to run ALTER TABLE commands to add them.
    # Example for one column (repeat for others if needed):
    # ALTER TABLE stock.tb_stock_fin_summary ADD COLUMN `OP_PROFIT_ANNOUNCED` BIGINT NULL;
    # ALTER TABLE stock.tb_stock_fin_summary ADD COLUMN `PRETAX_CONT_PROFIT` BIGINT NULL;
    # ... and so on for all new columns ...

    with engine.begin() as conn: 
        for row in data_list:
            keys = ", ".join(row.keys())
            values_placeholders = ", ".join(["%s"] * len(row))
            
            update_clauses = [f"{k}=VALUES({k})" for k in row.keys() if k not in ['STOCK_CD', 'BASE_YEAR']]
            updates = ", ".join(update_clauses)
            
            sql = f"""
                INSERT INTO stock.{FIN_SUMMARY_TABLE_NAME} ({keys}) 
                VALUES ({values_placeholders})
                ON DUPLICATE KEY UPDATE {updates};
            """
            try:
                conn.execute(sql, tuple(row.values()))
            except Exception as e:
                logging.error(f"데이터 삽입/업데이트 중 오류 발생 for {row.get('STOCK_CD')} - {row.get('BASE_YEAR')}: {e}")

    logging.info(f"{len(data_list)}건 저장 완료 (INSERT ... ON DUPLICATE KEY UPDATE)")

# --- 엑셀 저장 ---
def export_to_excel(data_list, target_year):
    if not data_list:
        logging.info("엑셀로 저장할 데이터 없음")
        return

    df = pd.DataFrame(data_list)
    os.makedirs(EXPORT_FOLDER, exist_ok=True)
    
    export_file_name = f'tb_stock_fin_summary_{target_year}_{EXPORT_DATE}.xlsx'
    save_path = os.path.join(EXPORT_FOLDER, export_file_name)

    try:
        df.to_excel(save_path, index=False)
        logging.info(f"엑셀 파일 저장 완료: {save_path}")
    except Exception as e:
        logging.error(f"엑셀 파일 저장 중 오류 발생: {e}")

# --- 메인 실행 ---
def main():
    stock_codes = fetch_stock_codes()
    logging.info(f"{len(stock_codes)}개 종목 분석 시작")

    while True:
        try:
            target_year_input = input("재무 데이터를 추출할 연도를 입력하세요 (예: 2024): ")
            target_year = int(target_year_input)
            if target_year <= 1900 or target_year > datetime.now().year + 2: 
                print("유효하지 않은 연도입니다. 현재 연도 주변의 값을 입력하세요.")
                continue
            break
        except ValueError:
            print("올바른 숫자 연도를 입력하세요.")
        except Exception as e:
            print(f"오류 발생: {e}")

    all_data = []
    for i, code in enumerate(stock_codes):
        time.sleep(random.uniform(0.5, 1.5)) 

        logging.info(f"[{code}] {target_year}년 재무 데이터 추출 시작 ({i+1}/{len(stock_codes)})")
        data = extract_fin_summary(code, target_year) 
        if data:
            all_data.append(data)

    insert_fin_summary_on_duplicate_key_update(all_data)
    export_to_excel(all_data, target_year) 
    logging.info("스크립트 실행 완료")

if __name__ == "__main__":
    main()
 