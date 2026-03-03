# 백테스팅 - RSI 전략이 과거에 통했는지 검증
# 매수: RSI 30 이하 / 매도: RSI 60 이상
import FinanceDataReader as fdr
import ta
import pandas as pd
from datetime import datetime


# =============================================
# 1. 주가 데이터 + RSI 계산
# =============================================
def get_data(ticker, start, end):
    df = fdr.DataReader(ticker, start, end)  # 주가 데이터 가져오기
    # RSI 계산 (14일 기준)
    df['RSI'] = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
    return df


# =============================================
# 2. 백테스팅 함수
# =============================================
def backtest(df, buy_rsi=30, sell_rsi=50, cash=10000000, stop_loss=-20):
    # buy_rsi   = 매수 기준 RSI
    # sell_rsi  = 매도 기준 RSI
    # cash      = 초기 자본금
    # stop_loss = 손절 기준 수익률 (기본 -10%)

    position = 0    # 보유 주식 수
    buy_price = 0   # 매수 가격
    trades = []     # 거래 기록

    for date, row in df.iterrows():
        rsi = row['RSI']
        price = row['Close']

        if pd.isna(rsi):
            continue

        # 매수 조건: 주식 없고 RSI 30 이하
        if position == 0 and rsi <= buy_rsi:
            position = int(cash / price)
            buy_price = price
            cash -= position * price
            trades.append({
                '날짜': date, '구분': '매수',
                '가격': price, 'RSI': round(rsi, 2), '수량': position
            })
            print(f"🔵 매수 | {date.date()} | {price:,}원 | RSI: {round(rsi,2)}")

        elif position > 0:
            # 현재 수익률 계산
            current_profit_rate = (price - buy_price) / buy_price * 100

            # 손절 조건: 수익률이 stop_loss 이하 (예: -10% 이하)
            if current_profit_rate <= stop_loss:
                sell_amount = position * price
                profit = sell_amount - (position * buy_price)
                cash += sell_amount
                trades.append({
                    '날짜': date, '구분': '손절매도',
                    '가격': price, 'RSI': round(rsi, 2),
                    '수량': position, '수익금': profit,
                    '수익률': round(current_profit_rate, 2)
                })
                print(f"🟡 손절 | {date.date()} | {price:,}원 | RSI: {round(rsi,2)} | 수익률: {round(current_profit_rate,2)}%")
                position = 0

            # 매도 조건: RSI 50 이상
            elif rsi >= sell_rsi:
                sell_amount = position * price
                profit = sell_amount - (position * buy_price)
                profit_rate = round((price - buy_price) / buy_price * 100, 2)
                cash += sell_amount
                trades.append({
                    '날짜': date, '구분': '매도',
                    '가격': price, 'RSI': round(rsi, 2),
                    '수량': position, '수익금': profit,
                    '수익률': profit_rate
                })
                print(f"🔴 매도 | {date.date()} | {price:,}원 | RSI: {round(rsi,2)} | 수익률: {profit_rate}%")
                position = 0

    return trades, cash


# =============================================
# 실행 코드
# =============================================

# 삼성전자 2024년 데이터 가져오기
ticker = "005930"
df = get_data(ticker, "20200101", "20241231")
print(f"데이터 수집 완료: {len(df)}개 거래일")
print(f"테스트 종목: 삼성전자 ({ticker})")
print(f"테스트 기간: 2024년 1월 ~ 2024년 12월")
print("-" * 50)

# 백테스팅 실행 (초기자본 1000만원)
trades, final_cash = backtest(df, buy_rsi=30, sell_rsi=50, cash=10000000)

# 결과 출력
print("-" * 50)
print(f"\n📊 백테스팅 결과")
print(f"초기 자본금: 10,000,000원")
print(f"최종 자본금: {int(final_cash):,}원")
print(f"총 수익금:   {int(final_cash - 10000000):,}원")
print(f"총 수익률:   {round((final_cash - 10000000) / 10000000 * 100, 2)}%")
print(f"총 거래 횟수: {len(trades)}번")