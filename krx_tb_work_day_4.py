import requests
import pandas as pd
from datetime import datetime
import io
from sqlalchemy import create_engine, text

# DB 설정
MYSQL_USER = "root"
MYSQL_PASSWORD = ""
MYSQL_HOST = "localhost"
MYSQL_DB = "stock"
TABLE_NAME = "tb_work_day"

# SQLAlchemy 엔진 생성
engine = create_engine(
    f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}/{MYSQL_DB}?charset=utf8"
)

# 삼성전자 거래일 데이터 조회
def get_samsung_trading_days(start_date, end_date):
    session = requests.Session()
    headers = {
        'User-Agent': 'Mozilla/5.0',
        'Referer': 'http://data.krx.co.kr/',
        'X-Requested-With': 'XMLHttpRequest'
    }

    otp_payload = {
        'tboxisuCd_finder_stkisu0_0': '005930/삼성전자',
        'isuCd': 'KR7005930003',
        'strtDd': start_date.replace('-', ''),
        'endDd': end_date.replace('-', ''),
        'csvxls_isNo': 'false',
        'name': 'fileDown',
        'url': 'dbms/MDC/STAT/standard/MDCSTAT01701'
    }

    try:
        otp_resp = session.post('http://data.krx.co.kr/comm/fileDn/GenerateOTP/generate.cmd', headers=headers, data=otp_payload)
        otp_resp.raise_for_status()
        otp = otp_resp.text.strip()

        download_resp = session.post('http://data.krx.co.kr/comm/fileDn/download_csv/download.cmd', headers=headers, data={'code': otp})
        download_resp.raise_for_status()
        csv_data = download_resp.content.decode('euc-kr')

        df = pd.read_csv(io.StringIO(csv_data))
        if '일자' not in df.columns:
            print("⚠️ '일자' 컬럼이 없습니다.")
            return None

        df = df.rename(columns={'일자': 'Date'})
        df['Date'] = pd.to_datetime(df['Date'], format='%Y/%m/%d', errors='coerce')
        df = df.dropna(subset=['Date'])
        return df[['Date']].drop_duplicates().sort_values('Date')

    except Exception as e:
        print(f"❌ 데이터 요청 실패: {e}")
        return None

# 거래일 DB 테이블 저장
def recreate_work_days(df):
    if df.empty:
        print("⚠️ 저장할 거래일이 없습니다.")
        return

    df = df.sort_values('Date').reset_index(drop=True)

    work_df = pd.DataFrame({
        'WORK_DAY': df['Date'].dt.strftime('%Y%m%d'),
        'WORK_YN': 'Y',
        'WORK_DIV': 'stock',
        'WORK_SEQ': df.index + 1
    })

    try:
        with engine.connect() as conn:
            with conn.begin():
                conn.execute(text(f"DELETE FROM {TABLE_NAME} WHERE WORK_DIV = 'stock'"))
                print(f"✅ 기존 데이터 삭제 완료")

                # 수동 REPLACE INTO 사용 (to_sql은 REPLACE 미지원)
                insert_sql = text(f"""
                    REPLACE INTO {TABLE_NAME} (WORK_DAY, WORK_YN, WORK_DIV, WORK_SEQ)
                    VALUES (:WORK_DAY, :WORK_YN, :WORK_DIV, :WORK_SEQ)
                """)
                for _, row in work_df.iterrows():
                    conn.execute(insert_sql, row.to_dict())

        print(f"✅ 총 {len(work_df)}개의 거래일이 저장되었습니다.")

    except Exception as e:
        print(f"❌ DB 저장 실패: {e}")

# 메인 실행
if __name__ == '__main__':
    start_date = '2025-01-01'
    end_date = datetime.now().strftime('%Y-%m-%d')

    print(f"📅 삼성전자 거래일 조회: {start_date} ~ {end_date}")
    trading_days = get_samsung_trading_days(start_date, end_date)

    if trading_days is not None:
        recreate_work_days(trading_days)
        trading_days.to_csv('samsung_work_days.csv', index=False, encoding='utf-8-sig')
        print("📁 'samsung_work_days.csv' 저장 완료.")
    else:
        print("❌ 거래일 데이터를 불러오지 못했습니다.")