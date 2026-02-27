# pykrx 대신 KIND + FinanceDataReader 조합으로 스크리닝
import pandas as pd
import requests
from io import StringIO
import time
import ta
import FinanceDataReader as fdr


# 코스피 전 종목 리스트 가져오는 함수
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


# RSI 계산 함수
def get_rsi(ticker, start="20250101", end="20250221"):
    try:
        df = fdr.DataReader(ticker, start, end)
        if len(df) < 20:
            return None
        rsi = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        return round(rsi.iloc[-1], 2)
    except:
        return None


# 종목 리스트 가져오기
tickers, df = get_kospi_tickers_from_kind()
print(f"코스피 전체 종목 수: {len(tickers)}개")

result = []
print("스크리닝 시작...")

for i, ticker in enumerate(tickers):      # [:50] 제거 → 전체 종목
    rsi = get_rsi(ticker)
    if rsi is not None and rsi <= 35:      # 35 이하로 다시 조건 강화
        name = df[df['종목코드'].astype(str).str.zfill(6) == ticker]['회사명'].values
        name = name[0] if len(name) > 0 else ticker
        result.append({'종목코드': ticker, '종목명': name, 'RSI': rsi})
        print(f"✅ {name} ({ticker}) RSI: {rsi}")
    time.sleep(0.3)

print(f"\n총 {len(result)}개 종목 발견!")