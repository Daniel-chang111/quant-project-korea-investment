import pandas as pd
import os
from datetime import datetime, timedelta
import FinanceDataReader as fdr

# =============================================
# 전략 조건 설정
# =============================================
BUY_CHANGE_RATE    = 5.0       # 당일 등락률 +5% 이상
BUY_VOLUME_RATIO   = 2.0       # 당일 거래량 >= 전일 x 2
STOP_LOSS          = -5.0      # 손절 -5%
TAKE_PROFIT        = 10.0      # 익절 +10%
TOP_N              = 50        # 거래대금 상위 50위
TOTAL_ASSET        = 10000000  # 초기 자산 1000만원
BUY_RATIO          = 0.02      # 매수금액 전체 자산의 2%
COMMISSION         = 0.00015   # 수수료 0.015%
TAX                = 0.0018    # 매도세 0.18%
SLIPPAGE           = 0.001     # 슬리피지 0.1%
VOLATILITY_WINDOW  = 35        # 변동성 체크 봉 수
VOLATILITY_RATE    = 9.5       # 종가 대비 고가 %
VOLATILITY_COUNT   = 3         # 발생 횟수

DAILY_DIR = "data_backtest/daily"
MIN_DIR   = "data_backtest/min"

EXCLUDE_KEYWORDS = ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'HANARO',
                    'ARIRANG', 'KOSEF', 'FOCUS', 'SOL', 'ACE', 'RISE', 'PLUS']


# =============================================
# 1. 전종목 일봉 데이터 로드
# =============================================
def load_all_daily():
    all_data = {}
    files = [f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")]

    for file in files:
        parts = file.replace(".csv", "").split("_")
        ticker = parts[0]
        name = parts[1] if len(parts) > 1 else ticker

        if any(k in name.upper() for k in EXCLUDE_KEYWORDS):
            continue

        df = pd.read_csv(f"{DAILY_DIR}/{file}")
        if len(df) == 0:
            continue

        df['날짜'] = df['날짜'].astype(str).str.strip()

        for col in ['시가', '고가', '저가', '종가', '거래량', '거래대금']:
            if col in df.columns:
                df[col] = df[col].astype(str).str.lstrip('-').str.strip()
                df[col] = pd.to_numeric(df[col], errors='coerce').abs()

        df = df.dropna()
        df = df.sort_values('날짜').reset_index(drop=True)
        all_data[ticker] = {'name': name, 'df': df}

    print(f"일봉 데이터 로드 완료: {len(all_data)}개 종목")
    return all_data


# =============================================
# 2. 날짜별 거래대금 상위 50위 추출
# =============================================
def get_top_n_by_date(all_daily, date_str):
    records = []
    for ticker, item in all_daily.items():
        df = item['df']
        row = df[df['날짜'] == date_str]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        if '거래대금' not in row or pd.isna(row['거래대금']):
            continue
        records.append({
            '종목코드': ticker,
            '종목명': item['name'],
            '거래대금': row['거래대금'],
            '종가': row['종가'],
            '거래량': row['거래량']
        })

    if len(records) == 0:
        return []

    df_rank = pd.DataFrame(records)
    df_rank = df_rank.sort_values('거래대금', ascending=False)
    return df_rank.head(TOP_N).to_dict('records')


# =============================================
# 3. 변동성 필터
# =============================================
def is_high_volatility(ticker, all_daily, date_str):
    if ticker not in all_daily:
        return False

    df = all_daily[ticker]['df']
    df_before = df[df['날짜'] < date_str].tail(VOLATILITY_WINDOW)

    if len(df_before) < VOLATILITY_WINDOW:
        return False

    df_before = df_before.copy()
    df_before['고가상승률'] = (df_before['고가'] - df_before['종가']) / df_before['종가'] * 100
    count = (df_before['고가상승률'] >= VOLATILITY_RATE).sum()

    return count >= VOLATILITY_COUNT


# =============================================
# 4. 매수 조건 체크
# =============================================
def check_buy_condition(ticker, all_daily, date_str):
    if ticker not in all_daily:
        return False, 0, 0

    df = all_daily[ticker]['df']
    today_rows = df[df['날짜'] == date_str]
    if len(today_rows) == 0:
        return False, 0, 0

    today_idx = today_rows.index[0]
    if today_idx == 0:
        return False, 0, 0

    prev  = df.iloc[today_idx - 1]
    today = today_rows.iloc[0]

    prev_close  = prev['종가']
    prev_volume = prev['거래량']

    if prev_close == 0 or prev_volume == 0:
        return False, 0, 0

    today_close  = today['종가']
    today_volume = today['거래량']

    change_rate = (today_close - prev_close) / prev_close * 100

    # 상한가 제외
    if change_rate >= 29.0:
        return False, 0, 0

    condition1 = change_rate >= BUY_CHANGE_RATE
    condition2 = today_volume >= prev_volume * BUY_VOLUME_RATIO

    return condition1 and condition2, change_rate, prev_close


# =============================================
# 5. 분봉 데이터 로드
# =============================================
def load_min_data(ticker):
    files = [f for f in os.listdir(MIN_DIR)
             if f.startswith(f"{ticker}_") and f.endswith(".csv")]
    if len(files) == 0:
        return None

    df = pd.read_csv(f"{MIN_DIR}/{files[0]}")
    if len(df) == 0:
        return None

    df['시간'] = df['시간'].astype(str).str.strip()

    for col in ['시가', '고가', '저가', '종가', '거래량']:
        if col in df.columns:
            df[col] = df[col].astype(str).str.lstrip('-').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce').abs()

    df = df.dropna()
    df = df.sort_values('시간').reset_index(drop=True)
    return df


# =============================================
# 6. 매도 시뮬레이션
# =============================================
def simulate_trade(df_min, buy_date, buy_price, buy_time):
    # 매수 시간 이후 분봉만 사용
    df_after = df_min[df_min['시간'].astype(str) >= buy_time].copy()

    if len(df_after) == 0:
        return buy_price, "데이터없음", buy_time

    sell_price  = None
    sell_reason = None
    sell_time   = None

    for _, row in df_after.iterrows():
        time_str = str(row['시간'])
        date_str = time_str[:8]
        hhmm     = time_str[8:12]

        # 익절 체크 (+10%)
        high_profit = (row['고가'] - buy_price) / buy_price * 100
        if high_profit >= TAKE_PROFIT:
            sell_price  = round(buy_price * (1 + TAKE_PROFIT / 100))
            sell_reason = "익절"
            sell_time   = time_str
            break

        # 손절 체크 (-5%)
        low_profit = (row['저가'] - buy_price) / buy_price * 100
        if low_profit <= STOP_LOSS:
            sell_price  = round(buy_price * (1 + STOP_LOSS / 100))
            sell_reason = "손절"
            sell_time   = time_str
            break

        # 다음날 09:10 매도
        if date_str > buy_date and hhmm >= "0910":
            sell_price  = row['시가']
            sell_reason = "시간매도"
            sell_time   = time_str
            break

    if sell_price is None:
        sell_price  = df_after['종가'].iloc[-1]
        sell_reason = "종가매도"
        sell_time   = str(df_after['시간'].iloc[-1])

    return sell_price, sell_reason, sell_time


# =============================================
# 7. 백테스트 실행
# =============================================
def run_backtest(start_date, end_date):
    print(f"\n{'='*60}")
    print(f"📊 백테스트 시작: {start_date} ~ {end_date}")
    print(f"{'='*60}")

    all_daily = load_all_daily()

    # 거래일 목록
    sample_ticker = list(all_daily.keys())[0]
    all_dates = all_daily[sample_ticker]['df']['날짜'].tolist()
    trade_dates = [d for d in all_dates if start_date <= d <= end_date]

    total_asset = TOTAL_ASSET
    results     = []

    for date_str in trade_dates:
        top_n = get_top_n_by_date(all_daily, date_str)
        if len(top_n) == 0:
            continue

        for item in top_n:
            ticker = item['종목코드']
            name   = item['종목명']

            # 변동성 필터
            if is_high_volatility(ticker, all_daily, date_str):
                continue

            # 매수 조건 체크
            buy_ok, change_rate, prev_close = check_buy_condition(
                ticker, all_daily, date_str
            )
            if not buy_ok:
                continue

            # 분봉 데이터 로드
            df_min = load_min_data(ticker)
            if df_min is None:
                continue

            # 당일 분봉
            today_min = df_min[df_min['시간'].str[:8] == date_str].copy()

            # 오전 9시 10분 이전 데이터 없으면 건너뛰기 (데이터 불완전)
            if len(today_min) == 0:
                continue
            first_time = str(today_min['시간'].iloc[0])[8:12]
            if first_time > '0910':
                continue

            if len(today_min) < 2:
                continue

            # 전일 거래량
            prev_volume_df = all_daily[ticker]['df']
            prev_vol_rows  = prev_volume_df[prev_volume_df['날짜'] < date_str]
            if len(prev_vol_rows) == 0:
                continue
            prev_volume = prev_vol_rows['거래량'].iloc[-1]

            # 매수 시점 찾기 (조건 충족 다음 봉 시가)
            buy_price = None
            buy_time  = None

            for idx in range(len(today_min) - 1):
                row    = today_min.iloc[idx]
                price  = row['종가']
                cr     = (price - prev_close) / prev_close * 100
                volume = today_min.iloc[:idx+1]['거래량'].sum()

                if cr >= BUY_CHANGE_RATE and volume >= prev_volume * BUY_VOLUME_RATIO:
                    next_row  = today_min.iloc[idx + 1]
                    buy_price = round(next_row['시가'] * (1 + SLIPPAGE))
                    buy_time  = str(next_row['시간'])
                    break

            if buy_price is None or buy_time is None:
                continue

            # 매수 수량
            shares = int((total_asset * BUY_RATIO) / buy_price)
            if shares == 0:
                continue

            # 매수 비용 (수수료 포함)
            buy_cost = buy_price * shares * (1 + COMMISSION)

            # 매도 시뮬레이션
            sell_price, sell_reason, sell_time = simulate_trade(
                df_min, date_str, buy_price, buy_time
            )

            # 매도 수익 (수수료 + 세금 포함)
            sell_revenue = sell_price * shares * (1 - COMMISSION - TAX)

            profit      = sell_revenue - buy_cost
            profit_rate = round((sell_price - buy_price) / buy_price * 100, 2)
            total_asset += profit

            # 시간 포맷 정리
            buy_hhmm  = f"{buy_time[8:10]}:{buy_time[10:12]}"
            sell_date = sell_time[:8]
            sell_hhmm = f"{sell_time[8:10]}:{sell_time[10:12]}"

            print(f"[{date_str}] 🔵 {name} | 등락률:{round(change_rate,1)}% | "
                  f"매수:{buy_price:,}원({buy_hhmm}) → "
                  f"매도:{sell_price:,}원({sell_date} {sell_hhmm}) | "
                  f"수익률:{profit_rate}% | {sell_reason}")

            results.append({
                '날짜'    : date_str,
                '종목명'  : name,
                '종목코드': ticker,
                '매수가'  : buy_price,
                '매수시간': buy_hhmm,
                '매도가'  : sell_price,
                '매도시간': f"{sell_date} {sell_hhmm}",
                '수익률'  : profit_rate,
                '수익금'  : int(profit),
                '매도사유': sell_reason
            })

    # 최종 결과
    # 최종 결과
    print(f"\n{'='*60}")
    print(f"📊 백테스트 결과")
    print(f"초기 자산:    {TOTAL_ASSET:,}원")
    print(f"최종 자산:    {int(total_asset):,}원")
    print(f"총 수익금:    {int(total_asset - TOTAL_ASSET):,}원")
    print(f"총 수익률:    {round((total_asset - TOTAL_ASSET) / TOTAL_ASSET * 100, 2)}%")
    print(f"총 거래 횟수: {len(results)}번")

    if results:
        result_df = pd.DataFrame(results)
        win = len(result_df[result_df['수익률'] > 0])
        print(f"승률:         {round(win / len(results) * 100, 1)}%")
        filename = f"backtest_result_{start_date}_{end_date}.xlsx"
        result_df.to_excel(filename, index=False)
        print(f"✅ 저장 완료: {filename}")

    # =============================================
    # 코스피 벤치마크 비교 (Look-ahead bias 방지)
    # → 시작일 종가로 매수, 종료일 종가로 매도
    # → 중간 데이터 참조 없음
    # =============================================
    try:
        df_kospi = fdr.DataReader('KS11', start_date, end_date)
        df_kospi = df_kospi.sort_index()

        kospi_start = df_kospi['Close'].iloc[0]   # 시작일 종가
        kospi_end   = df_kospi['Close'].iloc[-1]  # 종료일 종가
        kospi_return = round((kospi_end - kospi_start) / kospi_start * 100, 2)

        print(f"\n📈 코스피 벤치마크 비교")
        print(f"코스피 시작({df_kospi.index[0].strftime('%Y-%m-%d')}): {round(kospi_start, 2):,}pt")
        print(f"코스피 종료({df_kospi.index[-1].strftime('%Y-%m-%d')}): {round(kospi_end, 2):,}pt")
        print(f"코스피 수익률: {kospi_return}%")
        print(f"전략 초과수익: {round((total_asset - TOTAL_ASSET) / TOTAL_ASSET * 100 - kospi_return, 2)}%")

    except Exception as e:
        print(f"코스피 데이터 조회 실패: {e}")

    if results:
        result_df = pd.DataFrame(results)
        win = len(result_df[result_df['수익률'] > 0])
        print(f"승률:         {round(win / len(results) * 100, 1)}%")
        filename = f"backtest_result_{start_date}_{end_date}.xlsx"
        result_df.to_excel(filename, index=False)
        print(f"✅ 저장 완료: {filename}")


# =============================================
# 실행
# =============================================
run_backtest("20250304", "20251231")