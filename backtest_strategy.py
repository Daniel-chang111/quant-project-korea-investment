import pandas as pd
import os
from datetime import datetime


# =============================================
# 전략 조건 설정
# =============================================
BUY_CHANGE_RATE = 5.0       # 당일 등락률 +5% 이상
BUY_VOLUME_RATIO = 2.0      # 당일 거래량 >= 전일 거래량 x 2
STOP_LOSS = -5.0            # 스탑로스 -5%
TAKE_PROFIT = 10.0          # 익절 +10%
NEXT_DAY_SELL_TIME = "0910" # 다음날 09:10 매도
TOTAL_ASSET = 10000000      # 초기 자산 1000만원
BUY_RATIO = 0.02            # 매수금액 전체 자산의 2%
VOLATILITY_WINDOW = 35      # 변동성 체크 봉 수
VOLATILITY_RATE = 9.5       # 변동성 기준 (종가 대비 고가 %)
VOLATILITY_COUNT = 3        # 변동성 발생 횟수


# =============================================
# 1. 변동성 필터 (일봉 기준)
# =============================================
def is_high_volatility(ticker):
    try:
        # 최근 35일 일봉 데이터 가져오기
        df = fdr.DataReader(ticker, start="20250101")
        if len(df) < VOLATILITY_WINDOW:
            return False

        # 최근 35봉만 사용
        df = df.tail(VOLATILITY_WINDOW)

        # 종가 대비 고가 상승률 계산
        df['고가상승률'] = (df['High'] - df['Close']) / df['Close'] * 100

        # 9.5% 이상 발생 횟수
        count = (df['고가상승률'] >= VOLATILITY_RATE).sum()

        if count >= VOLATILITY_COUNT:
            return True  # 변동성 큰 종목 → 제외
        return False

    except:
        return False


# =============================================
# 2. 매수 조건 체크 (분봉 데이터 기준)
# =============================================
def check_buy_condition(df_min, df_day):
    # df_min = 당일 분봉 데이터
    # df_day = 전일 포함 일봉 데이터

    if len(df_day) < 2:
        return False, None

    # 전일 종가
    prev_close = df_day['Close'].iloc[-2]

    # 당일 첫 번째 분봉 시가
    today_open = df_min['시가'].iloc[0]

    # 등락률 계산 (당일 현재가 기준)
    today_close = df_min['종가'].iloc[-1]
    change_rate = (today_close - prev_close) / prev_close * 100

    # 전일 거래량
    prev_volume = df_day['Volume'].iloc[-2]

    # 당일 거래량
    today_volume = df_min['거래량'].sum()

    # 상한가 체크 (등락률 29% 이상이면 상한가로 판단)
    is_upper_limit = change_rate >= 29.0

    # 조건 체크
    condition1 = change_rate >= BUY_CHANGE_RATE        # 등락률 +5% 이상
    condition2 = today_volume >= prev_volume * BUY_VOLUME_RATIO  # 거래량 2배 이상
    condition3 = not is_upper_limit                     # 상한가 아님

    return condition1 and condition2 and condition3, change_rate


# =============================================
# 3. 백테스트 실행
# =============================================
def run_backtest():
    data_dir = "data"
    files = os.listdir(data_dir)
    csv_files = [f for f in files if f.endswith(".csv")]

    print(f"총 {len(csv_files)}개 종목 백테스트 시작...")
    print("-" * 60)

    total_asset = TOTAL_ASSET
    results = []

    for file in csv_files:
        # 파일명에서 종목코드, 종목명 추출
        parts = file.replace(".csv", "").split("_")
        ticker = parts[0]
        name = parts[1] if len(parts) > 1 else ticker

        # 분봉 데이터 로드
        df_min = pd.read_csv(f"{data_dir}/{file}")
        if len(df_min) < 10:
            continue

        # 시간 컬럼을 문자열로 변환
        df_min['시간'] = df_min['시간'].astype(str)

        # 일봉 데이터 로드 (전일 거래량 비교용)
        try:
            df_day = fdr.DataReader(ticker, start="20250101")
        except:
            continue

        if len(df_day) < 2:
            continue

        # 변동성 필터 체크
        if is_high_volatility(ticker):
            print(f"❌ 변동성 제외: {name}")
            continue

        # 매수 조건 체크
        buy_ok, change_rate = check_buy_condition(df_min, df_day)
        if not buy_ok:
            continue

        # 매수 실행
        buy_amount = total_asset * BUY_RATIO        # 전체 자산의 2%
        buy_price = df_min['종가'].iloc[-1]          # 현재가에 매수
        shares = int(buy_amount / buy_price)         # 매수 수량

        if shares == 0:
            continue

        print(f"🔵 매수: {name} ({ticker}) | 등락률: {round(change_rate,2)}% | 매수가: {buy_price:,}원")

        # 매도 시뮬레이션
        sell_price = None
        sell_reason = None

        for _, row in df_min.iterrows():
            price = row['종가']
            time_str = str(row['시간'])[-6:-2]  # HHMM 추출

            # 수익률 계산
            profit_rate = (price - buy_price) / buy_price * 100

            # 익절 조건 (+10%)
            if profit_rate >= TAKE_PROFIT:
                sell_price = price
                sell_reason = "익절"
                break

            # 손절 조건 (-5%)
            if profit_rate <= STOP_LOSS:
                sell_price = price
                sell_reason = "손절"
                break

            # 다음날 09:10 매도 (분봉 데이터 마지막 부분에서 체크)
            if time_str >= NEXT_DAY_SELL_TIME:
                sell_price = price
                sell_reason = "시간매도"
                break

        if sell_price is None:
            sell_price = df_min['종가'].iloc[-1]
            sell_reason = "종가매도"

        # 수익 계산
        profit = (sell_price - buy_price) * shares
        profit_rate = round((sell_price - buy_price) / buy_price * 100, 2)
        total_asset += profit

        print(f"🔴 매도: {name} | 매도가: {sell_price:,}원 | 수익률: {profit_rate}% | 사유: {sell_reason}")

        results.append({
            '종목명': name,
            '종목코드': ticker,
            '매수가': buy_price,
            '매도가': sell_price,
            '수익률': profit_rate,
            '수익금': profit,
            '매도사유': sell_reason
        })

    # 최종 결과 출력
    print("\n" + "=" * 60)
    print(f"📊 백테스트 결과")
    print(f"초기 자산:   {TOTAL_ASSET:,}원")
    print(f"최종 자산:   {int(total_asset):,}원")
    print(f"총 수익금:   {int(total_asset - TOTAL_ASSET):,}원")
    print(f"총 수익률:   {round((total_asset - TOTAL_ASSET) / TOTAL_ASSET * 100, 2)}%")
    print(f"총 거래 횟수: {len(results)}번")

    if results:
        result_df = pd.DataFrame(results)
        print(f"승률:        {round(len(result_df[result_df['수익률'] > 0]) / len(result_df) * 100, 1)}%")
        today = datetime.today().strftime("%Y%m%d")
        result_df.to_excel(f"backtest_result_{today}.xlsx", index=False)
        print(f"✅ 결과 저장 완료! → backtest_result_{today}.xlsx")


# =============================================
# 실행
# =============================================
run_backtest()