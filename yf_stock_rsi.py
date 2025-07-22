import yfinance as yf
import pandas as pd
import datetime as dt

def get_rsi(data: pd.DataFrame, period: int = 14) -> pd.Series:
    delta = data['Close'].diff()
    gain = delta.where(delta > 0, 0.0)
    loss = -delta.where(delta < 0, 0.0)

    avg_gain = gain.rolling(window=period, min_periods=period).mean()
    avg_loss = loss.rolling(window=period, min_periods=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi

def fetch_and_calculate_rsi(ticker: str, start_date: str, end_date: str):
    # Yahoo Finance에서 데이터 가져오기
    data = yf.download(ticker, start=start_date, end=end_date)
    
    if data.empty:
        print("데이터를 불러올 수 없습니다. 종목 코드를 확인하세요.")
        return

    data['RSI'] = get_rsi(data)

    # NaN 제거 후 결과 출력
    data = data.dropna(subset=['RSI'])

    print(data[['Close', 'RSI']])
    return data

if __name__ == "__main__":
    ticker = "AAPL"  # 원하는 종목으로 변경 (예: "BTC-USD", "005930.KS")
    start_date = "2025-01-02"
    end_date = "2025-06-09"

    rsi_data = fetch_and_calculate_rsi(ticker, start_date, end_date)
    
    # 결과 저장 (선택 사항)
    rsi_data.to_csv(f"{ticker}_RSI_{start_date}_to_{end_date}.csv")