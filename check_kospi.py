import pandas as pd
import numpy as np
import FinanceDataReader as fdr

start_date = "20250304"
end_date   = "20251231"

COMMISSION = 0.00015
TAX        = 0.0018

start_dt = pd.to_datetime(start_date)
end_dt   = pd.to_datetime(end_date)
years    = (end_dt - start_dt).days / 365.25

df_kospi = None

# 1차: FinanceDataReader
try:
    df_kospi = fdr.DataReader('KS11', start_date, end_date)
    if df_kospi is None or len(df_kospi) == 0:
        raise Exception("데이터 없음")
    df_kospi = df_kospi.sort_index()
    print("✅ 코스피 데이터 로드 성공 (FinanceDataReader)")

except Exception as e:
    print(f"FinanceDataReader 실패: {e}")
    try:
        import requests
        from io import StringIO

        headers = {'User-Agent': 'Mozilla/5.0'}
        rows    = []

        for page in range(1, 50):
            url  = f"https://finance.naver.com/sise/sise_index_day.naver?code=KOSPI&page={page}"
            res  = requests.get(url, headers=headers)
            dfs  = pd.read_html(StringIO(res.text))
            df_p = dfs[0].dropna()
            df_p.columns = ['날짜', 'Close', '전일비', '등락률', '거래량', '거래대금']
            df_p['날짜']  = pd.to_datetime(df_p['날짜'])
            df_p['Close'] = pd.to_numeric(
                df_p['Close'].astype(str).str.replace(',', ''), errors='coerce'
            )
            df_p = df_p.dropna(subset=['Close'])
            rows.append(df_p)

            oldest = df_p['날짜'].min()
            if oldest <= pd.to_datetime(start_date):
                break

        df_kospi = pd.concat(rows).set_index('날짜').sort_index()
        df_kospi = df_kospi[
            (df_kospi.index >= pd.to_datetime(start_date)) &
            (df_kospi.index <= pd.to_datetime(end_date))
        ]
        print("✅ 코스피 데이터 로드 성공 (네이버 금융)")

    except Exception as e2:
        print(f"네이버 금융도 실패: {e2}")

# 결과 출력
if df_kospi is not None and len(df_kospi) > 0:
    kospi_start       = float(df_kospi['Close'].iloc[0])
    kospi_end         = float(df_kospi['Close'].iloc[-1])
    kospi_return      = round((kospi_end - kospi_start) / kospi_start * 100, 2)
    kospi_cost        = (COMMISSION * 2 + TAX) * 100
    kospi_return_real = round(kospi_return - kospi_cost, 2)
    kospi_cagr        = round(((kospi_end / kospi_start) ** (1 / years) - 1) * 100, 2)
    kospi_prices      = df_kospi['Close'].values.astype(float)
    kospi_peak        = np.maximum.accumulate(kospi_prices)
    kospi_dd          = (kospi_prices - kospi_peak) / kospi_peak * 100
    kospi_mdd         = round(float(kospi_dd.min()), 2)

    print(f"\n{'='*40}")
    print(f"📈 코스피 벤치마크 결과")
    print(f"{'='*40}")
    print(f"시작일:        {df_kospi.index[0].strftime('%Y-%m-%d')}")
    print(f"종료일:        {df_kospi.index[-1].strftime('%Y-%m-%d')}")
    print(f"시작 지수:     {kospi_start:,.2f}pt")
    print(f"종료 지수:     {kospi_end:,.2f}pt")
    print(f"수익률(비용전): {kospi_return}%")
    print(f"수익률(비용후): {kospi_return_real}%")
    print(f"CAGR:          {kospi_cagr}%")
    print(f"MDD:           {kospi_mdd}%")
else:
    print("❌ 코스피 데이터 로드 실패")