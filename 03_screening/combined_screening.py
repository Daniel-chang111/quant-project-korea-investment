# RSI + 재무 복합 조건 스크리닝
# 조건: RSI 35 이하 + PER 10 이하 + ROE 10 이상 + 거래량 2배 이상
import pandas as pd
import requests
from io import StringIO
import time
import ta
import FinanceDataReader as fdr
from datetime import datetime


# 1. 네이버 금융에서 PER, ROE 데이터 가져오기
def get_naver_fundamental():
    all_data = []
    url = "https://finance.naver.com/sise/sise_market_sum.naver"
    headers = {"User-Agent": "Mozilla/5.0"}
    print("재무 데이터 수집 중...")
    for page in range(1, 19):
        params = {"sosok": "0", "page": page}
        r = requests.get(url, params=params, headers=headers)
        r.encoding = "euc-kr"
        tables = pd.read_html(StringIO(r.text))
        df = tables[1]
        df = df.dropna(subset=['종목명'])
        all_data.append(df)
        time.sleep(0.3)
    result = pd.concat(all_data, ignore_index=True)
    result['PER'] = pd.to_numeric(result['PER'], errors='coerce')
    result['ROE'] = pd.to_numeric(result['ROE'], errors='coerce')
    print(f"재무 데이터 {len(result)}개 수집 완료!")
    return result


# 2. KIND에서 코스피 종목코드 + 회사명 가져오기
def get_kind_df():
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do"
    params = {"method": "download", "marketType": "stockMkt"}
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}
    r = requests.get(url, params=params, headers=headers, timeout=15)
    html = r.content.decode("euc-kr", errors="ignore")
    kind_df = pd.read_html(StringIO(html))[0]
    # 종목코드 6자리로 맞추기
    kind_df['종목코드'] = kind_df['종목코드'].astype(str).str.zfill(6)
    return kind_df


# 3. RSI + 거래량 계산 함수
def get_rsi_volume(ticker, start="20250101", end="20250221"):
    try:
        df = fdr.DataReader(ticker, start, end)
        if len(df) < 20:
            return None, None
        rsi = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        rsi_value = round(rsi.iloc[-1], 2)
        volume_mean = df['Volume'].rolling(20).mean().iloc[-1]
        volume_last = df['Volume'].iloc[-1]
        if volume_mean == 0 or pd.isna(volume_mean):
            return None, None
        volume_ratio = round(volume_last / volume_mean, 2)
        return rsi_value, volume_ratio
    except:
        return None, None


# =============================================
# 실행 코드
# =============================================

# 1단계: 재무 데이터 가져오기
fund_df = get_naver_fundamental()

# 2단계: PER, ROE 조건 필터링
filtered_fund = fund_df[
    (fund_df['PER'] > 0) &
    (fund_df['PER'] <= 10) &
    (fund_df['ROE'] >= 10)
]
print(f"재무 조건 통과 종목: {len(filtered_fund)}개")

# 3단계: KIND에서 종목코드 가져오기
kind_df = get_kind_df()

# 4단계: 재무 통과 종목만 RSI 계산
result = []
print("RSI 스크리닝 시작...")

for i, row in filtered_fund.iterrows():
    name = row['종목명']
    matched = kind_df[kind_df['회사명'] == name]['종목코드'].values
    if len(matched) == 0:
        print(f"❌ 종목코드 못 찾음: {name}")
        continue
    ticker = matched[0]
    rsi, volume_ratio = get_rsi_volume(ticker)
    if rsi is not None and rsi <= 45 and volume_ratio >= 1.5:
        result.append({
            '종목명': name,
            '종목코드': ticker,
            'RSI': rsi,
            '거래량비율': volume_ratio,
            'PER': row['PER'],
            'ROE': row['ROE']
        })
        print(f"✅ {name} RSI: {rsi} PER: {row['PER']} ROE: {row['ROE']}")
    time.sleep(0.3)

# 5단계: 결과 출력 및 저장
print(f"\n🎯 복합 조건 통과 종목: {len(result)}개")
result_df = pd.DataFrame(result)
print(result_df.to_string(index=False))
today = datetime.today().strftime("%Y%m%d")
result_df.to_excel(f"03_screening/복합스크리닝_{today}.xlsx", index=False)
print(f"✅ 엑셀 저장 완료!")