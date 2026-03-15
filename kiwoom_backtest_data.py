import sys
import os
from pykiwoom.kiwoom import Kiwoom
from PyQt5.QtWidgets import QApplication
import pandas as pd
import time
from datetime import datetime

app = QApplication(sys.argv)
kiwoom = Kiwoom()
kiwoom.CommConnect(block=True)
print("✅ 로그인 성공!")

# =============================================
# 설정
# =============================================
START_DATE = "20250101"   # 수집 시작 날짜
END_DATE   = "20260308"   # 수집 종료 날짜
TICK       = 5            # 5분봉

EXCLUDE_KEYWORDS = ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'HANARO',
                    'ARIRANG', 'KOSEF', 'FOCUS', 'SOL', 'ACE', 'RISE']


# =============================================
# 1. 코스피 전종목 목록
# =============================================
def get_kospi_tickers():
    ticker_list = kiwoom.GetCodeListByMarket("0")  # 이미 리스트로 반환
    result = []
    for ticker in ticker_list:
        name = kiwoom.GetMasterCodeName(ticker).strip()
        if any(k in name.upper() for k in EXCLUDE_KEYWORDS):
            continue
        result.append({'종목코드': ticker, '종목명': name})
    print(f"코스피 종목 수 (ETF/ETN 제외): {len(result)}개")
    return result


# =============================================
# 2. 일봉 데이터 수집
# =============================================
def get_daily_data(ticker):
    df = kiwoom.block_request(
        "opt10081",
        종목코드=ticker,
        기준일자=END_DATE,
        수정주가구분=1,
        output="주식일봉차트조회",
        next=0
    )
    if df is None or len(df) == 0:
        return pd.DataFrame()

    df = df.rename(columns={
        '일자': '날짜',
        '현재가': '종가',
        '거래량': '거래량',
        '거래대금': '거래대금',
        '시가': '시가',
        '고가': '고가',
        '저가': '저가'
    })

    # 필요한 컬럼만
    cols = ['날짜', '시가', '고가', '저가', '종가', '거래량', '거래대금']
    df = df[[c for c in cols if c in df.columns]]

    # 숫자 변환 (음수 부호 제거)
    for col in ['시가', '고가', '저가', '종가', '거래량', '거래대금']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lstrip('-').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').abs()

    # 날짜 필터링
    df = df[df['날짜'].astype(str) >= START_DATE]
    df = df[df['날짜'].astype(str) <= END_DATE]

    return df.reset_index(drop=True)


# =============================================
# 3. 분봉 데이터 수집 (block_request 반복 호출)
# =============================================
def get_min_data(ticker):
    df_list = []

    # 첫 번째 요청
    df_block = kiwoom.block_request(
        "opt10080",
        종목코드=ticker,
        틱범위=str(TICK),
        수정주가구분=1,
        output="주식분봉차트조회",
        next=0
    )

    if df_block is None or len(df_block) == 0:
        return pd.DataFrame()

    df_list.append(df_block)

    # 반복 요청 (start_date까지)
    while kiwoom.tr_remained:
        df_block = kiwoom.block_request(
            "opt10080",
            종목코드=ticker,
            틱범위=str(TICK),
            수정주가구분=1,
            output="주식분봉차트조회",
            next=2
        )

        if df_block is None or len(df_block) == 0:
            break

        df_list.append(df_block)

        # ✅ 수정: iloc[-1] 로 가장 오래된 날짜 가져오기
        oldest = str(df_block['체결시간'].iloc[-1])[:8]
        if oldest <= START_DATE:
            break

        time.sleep(0.3)

    # 합치기
    df = pd.concat(df_list, ignore_index=True)

    # 컬럼 정리
    df = df.rename(columns={
        '체결시간': '시간',
        '현재가': '종가',
        '시가': '시가',
        '고가': '고가',
        '저가': '저가',
        '거래량': '거래량'
    })

    cols = ['시간', '시가', '고가', '저가', '종가', '거래량']
    df = df[[c for c in cols if c in df.columns]]

    # 숫자 변환
    for col in ['시가', '고가', '저가', '종가', '거래량']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lstrip('-').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').abs()

    # 날짜 필터링
    df['날짜'] = df['시간'].astype(str).str[:8]
    df = df[df['날짜'] >= START_DATE]
    df = df[df['날짜'] <= END_DATE]
    df = df.drop(columns=['날짜'])

    return df.reset_index(drop=True)

# =============================================
# 실행 코드
# =============================================
os.makedirs("data_backtest/daily", exist_ok=True)
os.makedirs("data_backtest/min", exist_ok=True)

print("코스피 종목 목록 가져오는 중...")
tickers = get_kospi_tickers()
total = len(tickers)

for i, item in enumerate(tickers):
    ticker = item['종목코드']
    name = item['종목명']

    daily_file = f"data_backtest/daily/{ticker}_{name}.csv"
    min_file   = f"data_backtest/min/{ticker}_{name}.csv"

    print(f"[{i+1}/{total}] {name} ({ticker}) 수집 중...")

    # 일봉 수집
    if not os.path.exists(daily_file):
        df_daily = get_daily_data(ticker)
        if len(df_daily) > 0:
            df_daily.to_csv(daily_file, index=False, encoding="utf-8-sig")
            print(f"  → 일봉 {len(df_daily)}개 저장!")
        time.sleep(0.4)

    # 분봉 수집
    if not os.path.exists(min_file):
        df_min = get_min_data(ticker)
        if len(df_min) > 0:
            df_min.to_csv(min_file, index=False, encoding="utf-8-sig")
            print(f"  → 분봉 {len(df_min)}개 저장!")
        time.sleep(0.4)

print("\n✅ 전체 데이터 수집 완료!")
print(f"일봉: data_backtest/daily/")
print(f"분봉: data_backtest/min/")