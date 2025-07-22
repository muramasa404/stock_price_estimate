import pandas as pd
from pykrx import stock
import sqlite3
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Function to fetch data from Naver Finance (placeholder for ROE, debt-to-equity)
def scrape_naver_finance(stock_code):
    try:
        url = f"https://finance.naver.com/item/main.nhn?code={stock_code}"
        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        
        # Placeholder: Extract ROE and debt-to-equity ratio
        # Note: Actual selectors depend on Naver Finance's HTML structure
        roe = None  # Replace with actual scraping logic
        debt_to_equity = None  # Replace with actual scraping logic
        return roe, debt_to_equity
    except Exception as e:
        logger.error(f"Error scraping Naver Finance for {stock_code}: {e}")
        return None, None

# Function to calculate PEG (placeholder, requires earnings growth data)
def calculate_peg(pe_ratio, earnings_growth):
    try:
        if pe_ratio and earnings_growth and earnings_growth != 0:
            return pe_ratio / earnings_growth
        return None
    except Exception as e:
        logger.error(f"Error calculating PEG: {e}")
        return None

# Function to initialize SQLite database
def init_db():
    conn = sqlite3.connect('krx_financial_data.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS financial_data (
            date TEXT,
            stock_code TEXT,
            stock_name TEXT,
            market_cap REAL,
            pbr REAL,
            peg REAL,
            roe REAL,
            debt_to_equity REAL,
            PRIMARY KEY (date, stock_code)
        )
    ''')
    conn.commit()
    return conn

# Function to fetch and store daily data
def collect_daily_data(date_str):
    try:
        # Initialize database
        conn = init_db()
        
        # Get all stock codes from KRX
        stock_codes = stock.get_market_ticker_list(date_str, market="ALL")
        logger.info(f"Found {len(stock_codes)} stocks for {date_str}")
        
        for code in stock_codes:
            try:
                # Get stock name
                stock_name = stock.get_market_ticker_name(code)
                
                # Fetch fundamental data (PBR, etc.)
                df_fund = stock.get_market_fundamental(date_str, date_str, code)
                
                # Fetch market cap
                df_cap = stock.get_market_cap(date_str, date_str, code)
                
                market_cap = df_cap['MARKET_CAP'].iloc[0] if not df_cap.empty else None
                pbr = df_fund['PBR'].iloc[0] if not df_fund.empty and 'PBR' in df_fund.columns else None
                pe_ratio = df_fund['PER'].iloc[0] if not df_fund.empty and 'PER' in df_fund.columns else None
                
                # Placeholder for PEG (requires earnings growth data)
                earnings_growth = None  # Need external source or calculation
                peg = calculate_peg(pe_ratio, earnings_growth)
                
                # Scrape ROE and debt-to-equity from Naver Finance
                roe, debt_to_equity = scrape_naver_finance(code)
                
                # Store data in database
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO financial_data 
                    (date, stock_code, stock_name, market_cap, pbr, peg, roe, debt_to_equity)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (date_str, code, stock_name, market_cap, pbr, peg, roe, debt_to_equity))
                conn.commit()
                
                logger.info(f"Stored data for {stock_name} ({code}) on {date_str}")
                
                # Avoid overwhelming servers
                time.sleep(1)
                
            except Exception as e:
                logger.error(f"Error processing {code}: {e}")
                continue
        
        conn.close()
        logger.info(f"Completed data collection for {date_str}")
        
    except Exception as e:
        logger.error(f"Error in collect_daily_data: {e}")
        if 'conn' in locals():
            conn.close()

# Main execution
if __name__ == "__main__":
    # Get current date in YYYYMMDD format
    current_date = datetime.now().strftime("%Y%m%d")
    
    # Collect data for the current date
    collect_daily_data(current_date)
    
    # Example: Collect data for a previous date (optional)
    # yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y%m%d")
    # collect_daily_data(yesterday)