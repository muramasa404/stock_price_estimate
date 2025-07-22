import requests
from bs4 import BeautifulSoup
import pandas as pd
import mysql.connector
from datetime import datetime

# 1. 시황 데이터 가져오기
def get_market_summary():
    url = "https://finance.naver.com/sise/"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    market_data = {}

    kospi_idx = soup.select_one("#KOSPI_now").text
    kospi_change = soup.select_one("#KOSPI_change").text
    kospi_rate = soup.select_one("#KOSPI_rate").text

    kosdaq_idx = soup.select_one("#KOSDAQ_now").text
    kosdaq_change = soup.select_one("#KOSDAQ_change").text
    kosdaq_rate = soup.select_one("#KOSDAQ_rate").text

    kospi200_idx = soup.select_one("#KPI200_now").text
    kospi200_change = soup.select_one("#KPI200_change").text
    kospi200_rate = soup.select_one("#KPI200_rate").text

    market_data['date'] = datetime.now().strftime("%Y-%m-%d")
    market_data['kospi'] = kospi_idx
    market_data['kospi_change'] = kospi_change
    market_data['kospi_rate'] = kospi_rate
    market_data['kosdaq'] = kosdaq_idx
    market_data['kosdaq_change'] = kosdaq_change
    market_data['kosdaq_rate'] = kosdaq_rate
    market_data['kospi200'] = kospi200_idx
    market_data['kospi200_change'] = kospi200_change
    market_data['kospi200_rate'] = kospi200_rate

    return market_data

# 2. 특징주 가져오기
def get_special_stocks():
    url = "https://finance.naver.com/sise/sise_upper.naver"  # 급등주 페이지
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    special_stocks = []
    stocks = soup.select("table.type_2 tr")

    for stock in stocks:
        cols = stock.select("td")
        if len(cols) >= 5:
            name = cols[1].get_text(strip=True)
            current_price = cols[2].get_text(strip=True)
            change = cols[3].get_text(strip=True)
            rate = cols[4].get_text(strip=True)
            special_stocks.append({
                'date': datetime.now().strftime("%Y-%m-%d"),
                'stock_name': name,
                'current_price': current_price,
                'change': change,
                'rate': rate
            })
    return special_stocks

# 3. MariaDB에 저장
def save_to_mariadb(market_summary, special_stocks):
    conn = mysql.connector.connect(
        host="localhost",   # ✅ 여기에 본인 DB 정보 입력
        user="your_username",
        password="your_password",
        database="your_database"
    )
    cursor = conn.cursor()

    # 테이블 생성 (없으면)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS market_summary (
            date DATE PRIMARY KEY,
            kospi VARCHAR(20),
            kospi_change VARCHAR(20),
            kospi_rate VARCHAR(20),
            kosdaq VARCHAR(20),
            kosdaq_change VARCHAR(20),
            kosdaq_rate VARCHAR(20),
            kospi200 VARCHAR(20),
            kospi200_change VARCHAR(20),
            kospi200_rate VARCHAR(20)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS special_stocks (
            id INT AUTO_INCREMENT PRIMARY KEY,
            date DATE,
            stock_name VARCHAR(100),
            current_price VARCHAR(20),
            change VARCHAR(20),
            rate VARCHAR(20)
        )
    """)

    # market_summary 저장
    cursor.execute("""
        REPLACE INTO market_summary 
        (date, kospi, kospi_change, kospi_rate, kosdaq, kosdaq_change, kosdaq_rate, kospi200, kospi200_change, kospi200_rate)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, (
        market_summary['date'],
        market_summary['kospi'],
        market_summary['kospi_change'],
        market_summary['kospi_rate'],
        market_summary['kosdaq'],
        market_summary['kosdaq_change'],
        market_summary['kosdaq_rate'],
        market_summary['kospi200'],
        market_summary['kospi200_change'],
        market_summary['kospi200_rate']
    ))

    # special_stocks 저장
    for stock in special_stocks:
        cursor.execute("""
            INSERT INTO special_stocks (date, stock_name, current_price, change, rate)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            stock['date'],
            stock['stock_name'],
            stock['current_price'],
            stock['change'],
            stock['rate']
        ))

    conn.commit()
    cursor.close()
    conn.close()
    print("✅ MariaDB 저장 완료.")

# 4. 메인 실행
if __name__ == "__main__":
    market_summary = get_market_summary()
    special_stocks = get_special_stocks()
    save_to_mariadb(market_summary, special_stocks)