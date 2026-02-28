# =============================================
# screening_v2.py - 캐시 기능 추가 버전
# 첫 실행: 전체 스크리닝 후 CSV 저장
# 두번째~: 저장된 CSV 바로 불러오기
# =============================================
import pandas as pd
import requests
from io import StringIO
import time
import ta
import FinanceDataReader as fdr
import os
from datetime import datetime


# 캐시 파일 경로
CACHE_FILE = "03_screening/cache_rsi.csv"


# 캐시 저장 함수
def save_cache(result):
    pd.DataFrame(result).to_csv(CACHE_FILE, index=False, encoding="utf-8-sig")
    print(f"💾 캐시 저장 완료!")


# 캐시 불러오기 함수
def load_cache():
    if os.path.exists(CACHE_FILE):   # 파일이 존재하면
        print("📂 저장된 데이터 불러오는 중...")
        return pd.read_csv(CACHE_FILE, dtype=str)
    return None                       # 파일 없으면 None 반환


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


# RSI + 거래량 계산 함수
def get_rsi(ticker, start="20250101", end="20250221"):
    try:
        df = fdr.DataReader(ticker, start, end)
        if len(df) < 20:
            return None, None
        rsi = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        rsi_value = round(rsi.iloc[-1], 2)
        volume_mean = df['Volume'].rolling(20).mean().iloc[-1]
        if volume_mean == 0 or pd.isna(volume_mean):
            return None, None
        volume_last = df['Volume'].iloc[-1]
        volume_ratio = round(volume_last / volume_mean, 2)
        return rsi_value, volume_ratio
    except:
        return None, None


# =============================================
# 실행 코드
# =============================================

# 종목 리스트 가져오기
tickers, df = get_kospi_tickers_from_kind()
print(f"코스피 전체 종목 수: {len(tickers)}개")

# 캐시 있으면 불러오고, 없으면 새로 스크리닝
cached = load_cache()

if cached is not None:
    # 저장된 데이터 바로 사용
    print(cached.to_string(index=False))
    print(f"\n총 {len(cached)}개 종목 발견!")

else:
    # 처음 실행 → 전체 스크리닝 후 저장
    result = []
    print("스크리닝 시작...")

    for i, ticker in enumerate(tickers):
        rsi, volume_ratio = get_rsi(ticker)
        if rsi is not None and rsi <= 35 and volume_ratio >= 2.0:
            name = df[df['종목코드'].astype(str).str.zfill(6) == ticker]['회사명'].values
            name = name[0] if len(name) > 0 else ticker
            result.append({'종목코드': ticker, '종목명': name, 'RSI': rsi, '거래량비율': volume_ratio})
            print(f"✅ {name} ({ticker}) RSI: {rsi} 거래량비율: {volume_ratio}")
        time.sleep(0.3)

    print(f"\n총 {len(result)}개 종목 발견!")
    save_cache(result)  # 결과 CSV 저장

    # 엑셀로도 저장
    today = datetime.today().strftime("%Y%m%d")
    pd.DataFrame(result).to_excel(f"03_screening/RSI_스크리닝_{today}.xlsx", index=False)
    print(f"✅ 엑셀 저장 완료! → RSI_스크리닝_{today}.xlsx")