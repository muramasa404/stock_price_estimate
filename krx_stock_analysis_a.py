
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import text  # Make sure this is at the top

import getpass

import seaborn as sns
import statsmodels.api as sm
from sklearn.preprocessing import StandardScaler
from sklearn.linear_model import LinearRegression
import matplotlib.pyplot as plt

from datetime import datetime
import os, sys



def fetch_data(base_dt, engine):
    query = """
    with trx_base_dt as
    (
        select
            a.base_dt
            ,b.work_seq
            ,b.work_seq + 1 as next_work_seq
        from
            stock.tb_stock_inv_trx_cnt a
            ,stock.tb_work_day b
        where 1=1
            and a.base_dt = :base_dt
            and b.work_day = a.base_dt
        group by work_day
            ,work_seq
    )
    ,trx_next_base_dt as
    (
        select
             b1.*
            ,b2.work_day as next_base_dt
        from
            trx_base_dt b1
            ,stock.tb_work_day b2
        where 1=1
            and b1.next_work_seq = b2.work_seq
    )
    , trx_net_buy_rank as (
    
           SELECT
                BASE_DT, STOCK_CD,
                SUM(CASE WHEN INV_DIV = '7050' THEN RANK_AMT ELSE 0 END) AS INV_RANK_AMT,
                SUM(CASE WHEN INV_DIV = '9000' THEN RANK_AMT ELSE 0 END) AS FOR_RANK_AMT
            FROM stock.TB_INV_NET_BUY_DAY
            WHERE BASE_DT = :base_dt
            GROUP BY BASE_DT, STOCK_CD
    )
    select
        a.BASE_DT
        ,a.STOCK_CD
        ,a.STOCK_NM
        ,a.INST_CNT
        ,a.INST_CON_CNT
        ,a.FORE_CNT
        ,a.FORE_CON_CNT
        ,a.BUY_CON_CNT
        ,a.AVG_TRX_QTY
        ,a.D1_TRX_QTY
        ,a.D7_AVG_TRX_RATE
        ,a.BUY_GRADE
        ,c.BASE_DT as NEXT_BASE_DT
        ,c.MRKT_DIV
        ,c.CLOSE_PRICE
        ,c.PRICE_GAP
        ,c.PRICE_GAP_RATE
        ,c.OPEN_PRICE
        ,c.HIGH_PRICE
        ,c.LOW_PRICE
        ,c.TRADE_QTY
        ,c.TRADE_AMT
        ,c.MRKT_CAPITAL
        ,c.ISSUE_STOCK_QTY
    from
        trx_next_base_dt x
        ,stock.tb_stock_inv_trx_cnt a
        ,stock.tb_stock_day_price c
        ,trx_net_buy_rank d
    where 1=1
        and a.base_dt = x.base_dt
        and c.base_dt = x.next_base_dt
        and c.stock_cd = a.stock_cd

        and d.base_dt = x.base_dt
        and d.stock_cd = a.stock_cd
        and d.INV_RANK_AMT < 20
        and d.FOR_RANK_AMT < 20
    """
    try:
        df = pd.read_sql(text(query), engine, params={'base_dt': base_dt})
        return df
    except Exception as e:
        print(f"Error fetching data: {e}")
        return None


def analyze_data(df, base_dt):
    # 분석 대상 변수 설정
    target = 'PRICE_GAP_RATE'
    exclude_cols = ['BASE_DT', 'STOCK_CD', 'STOCK_NM', 'BUY_GRADE', 'NEXT_BASE_DT', 'MRKT_DIV']
    features = [col for col in df.columns if col not in exclude_cols + [target]]

    # 결측 제거
    data = df[features + [target]].dropna()

    # 📌 다변량 회귀분석 (statsmodels OLS)
    X = sm.add_constant(data[features])
    y = data[target]
    model = sm.OLS(y, X).fit()

    # 📌 표준화 회귀계수 계산
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(data[features])
    model_std = LinearRegression().fit(X_scaled, y)
    standardized_coeffs = pd.Series(model_std.coef_, index=features)

    # 📌 상관행렬 히트맵
    corr = data.corr()

    # 📌 엑셀로 저장
    save_dir = r"D:\python_proj\venv_stock\stock_file"
    os.makedirs(save_dir, exist_ok=True)

    file_name = f"krx_analysis_b_{base_dt}.xlsx"
    file_path = os.path.join(save_dir, file_name)


    with pd.ExcelWriter(file_name, engine='openpyxl') as writer:
        df.to_excel(writer, sheet_name='RawData', index=False)
        pd.DataFrame({
            'Variable': model.params.index,
            'Coefficient': model.params.values,
            'P-value': model.pvalues.values,
            'StdErr': model.bse.values
        }).to_excel(writer, sheet_name='Regression', index=False)

        pd.DataFrame({
            'Variable': standardized_coeffs.index,
            'Standardized_Coeff': standardized_coeffs.values
        }).to_excel(writer, sheet_name='Standardized Coef', index=False)

        corr.to_excel(writer, sheet_name='Correlation')

    print(f"Excel file saved: {file_name}")

    # 📌 비선형 시각화 (산점도 행렬)
    sns.pairplot(data[[target] + features], diag_kind='kde')
    plt.suptitle("Scatter Matrix with KDE", y=1.02)
    plt.tight_layout()
    plt.savefig(f"scatter_matrix_{base_dt}.png")
    plt.show()

    # 📌 상관계수 히트맵
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr, annot=True, fmt=".2f", cmap='coolwarm', square=True)
    plt.title("Correlation Heatmap")
    plt.tight_layout()
    plt.savefig(f"correlation_heatmap_{base_dt}.png")
    plt.show()




# 📌 날짜 입력 유효성 검사 함수
def validate_date(date_str):
    try:
        datetime.strptime(date_str, "%Y%m%d")
        return True
    except ValueError:
        print(f"[!] Invalid date format: {date_str}. Please use YYYYMMDD (e.g., 20250404).")
        return False


def main():
    # 사용자 입력 받기
    base_dt = input("Enter base date (YYYYMMDD): ")


    # DB 연결 설정
    engine = create_engine("mysql+mysqlconnector://root@localhost/stock?charset=utf8")

    # 데이터 가져오기
    df = fetch_data(base_dt, engine)

    if df is not None:
        # 이후 분석 로직은 그대로 유지 (예: df를 사용한 처리)
        print("Data fetched successfully:")
        print(df.head())

        # 📌 엑셀로 저장
        save_dir = r"D:\python_proj\venv_stock\stock_file"
        os.makedirs(save_dir, exist_ok=True)

        file_name = f"krx_analysis_{base_dt}.xlsx"
        file_path = os.path.join(save_dir, file_name)
        
        df.to_excel(file_path, index=False)

        print(f"[✔] 엑셀 파일 저장 완료: {file_path}")


        # 데이터 분석
        analyze_data(df, base_dt)

    else:
        print("Failed to fetch data.")

if __name__ == "__main__":
    main()

 