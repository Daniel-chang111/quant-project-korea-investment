# pykrx 대신 KIND + FinanceDataReader 조합으로 스크리닝
# RSI 스크리닝 기본버전 - 매번 코스피 전종목 수집
import pandas as pd
import requests
from io import StringIO
import time
import ta
import FinanceDataReader as fdr
import os
from datetime import datetime

def get_kospi_tickers_from_kind(timeout=15):
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do"
    params = {"method": "download", "marketType": "stockMkt"}
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    html = r.content.decode("euc-kr", errors="ignore")
    df = pd.read_html(StringIO(html))[0]
    code = df["종목코드"].astype(str).str.strip()
    tickers = code[code.str.match(r"^\d{6}$")].tolist()
    return tickers, df

def get_rsi(ticker, start="20250101", end="20250221"):
    try:
        df = fdr.DataReader(ticker, start, end)
        if len(df) < 20:
            return None, None
        rsi = ta.momentum.RSIIndicator(df["Close"], window=14).rsi()
        rsi_value = round(rsi.iloc[-1], 2)
        volume_mean = df["Volume"].rolling(20).mean().iloc[-1]
        volume_last = df["Volume"].iloc[-1]
        if volume_mean == 0 or pd.isna(volume_mean):
            return None, None
        volume_ratio = round(volume_last / volume_mean, 2)
        return rsi_value, volume_ratio
    except:
        return None, None

tickers, df = get_kospi_tickers_from_kind()
print(f"코스피 전체 종목 수: {len(tickers)}개")

result = []
print("스크리닝 시작...")

for i, ticker in enumerate(tickers):
    rsi, volume_ratio = get_rsi(ticker)
    if rsi is not None and rsi <= 35 and volume_ratio >= 2.0:
        name = df[df["종목코드"].astype(str).str.zfill(6) == ticker]["회사명"].values
        name = name[0] if len(name) > 0 else ticker
        result.append({"종목코드": ticker, "종목명": name, "RSI": rsi, "거래량비율": volume_ratio})
        print(f"✅ {name} ({ticker}) RSI: {rsi} 거래량비율: {volume_ratio}")
    time.sleep(0.3)

print(f"\n총 {len(result)}개 종목 발견!")
result_df = pd.DataFrame(result)
today = datetime.today().strftime("%Y%m%d")
result_df.to_excel(f"03_screening/RSI_스크리닝_{today}.xlsx", index=False)
print(f"✅ 엑셀 저장 완료! → RSI_스크리닝_{today}.xlsx")
