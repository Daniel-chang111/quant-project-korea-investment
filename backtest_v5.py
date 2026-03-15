import pandas as pd
import numpy as np
import os
import requests
import FinanceDataReader as fdr
from io import StringIO
from datetime import datetime

# =============================================
# 전략 조건 설정
# =============================================
BUY_CHANGE_RATE    = 5.0       # 당일 등락률 +5% 이상
BUY_VOLUME_RATIO   = 2.0       # 당일 거래량 >= 전일 x 2
STOP_LOSS          = -5.0      # 손절 -5%
TAKE_PROFIT        = 10.0      # 익절 +10%
THEME_CHANGE_RATE  = 3.0       # 테마 등락률 +3% 이상
TOP_N              = 50        # 거래대금 상위 50위
TOTAL_ASSET        = 10000000  # 초기 자산 1000만원
BUY_RATIO          = 0.02      # 매수금액 전체 자산의 2%
COMMISSION         = 0.00015   # 수수료 0.015%
TAX                = 0.0018    # 매도세 0.18%
SLIPPAGE           = 0.001     # 슬리피지 0.1%
VOLATILITY_WINDOW  = 35        # 변동성 체크 봉 수
VOLATILITY_RATE    = 9.5       # 종가 대비 고가 %
VOLATILITY_COUNT   = 3         # 발생 횟수

DAILY_DIR  = "data_backtest/daily"
MIN_DIR    = "data_backtest/min"
THEME_FILE = "data_theme/theme_stocks.csv"

EXCLUDE_KEYWORDS = ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'HANARO',
                    'ARIRANG', 'KOSEF', 'FOCUS', 'SOL', 'ACE', 'RISE', 'PLUS']


# =============================================
# 1. 전종목 일봉 데이터 로드
# =============================================
def load_all_daily():
    all_data = {}
    files = [f for f in os.listdir(DAILY_DIR) if f.endswith(".csv")]

    for file in files:
        parts  = file.replace(".csv", "").split("_")
        ticker = parts[0]
        name   = parts[1] if len(parts) > 1 else ticker

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
# 2. 테마별 종목 로드
# =============================================
def load_theme_stocks():
    if not os.path.exists(THEME_FILE):
        print("❌ 테마 데이터 없음")
        return {}

    df = pd.read_csv(THEME_FILE, encoding="utf-8-sig")
    df['종목명'] = df['종목명'].astype(str).str.strip()
    df['테마명'] = df['테마명'].astype(str).str.strip()

    theme_dict = {}
    for theme_name, group in df.groupby('테마명'):
        theme_dict[theme_name] = group['종목명'].tolist()

    print(f"테마 데이터 로드 완료: {len(theme_dict)}개 테마")
    return theme_dict


# =============================================
# 3. 종목명 → 종목코드 매핑 생성
# =============================================
def build_name_to_ticker(all_daily):
    name_to_ticker = {}
    for ticker, item in all_daily.items():
        name_to_ticker[item['name']] = ticker
    return name_to_ticker


# =============================================
# 4. 날짜별 테마 등락률 계산
# =============================================
def calc_theme_change_rate(theme_dict, all_daily, name_to_ticker, date_str):
    theme_rates = {}

    for theme_name, stock_names in theme_dict.items():
        rates = []
        for name in stock_names:
            ticker = name_to_ticker.get(name)
            if ticker is None or ticker not in all_daily:
                continue

            df         = all_daily[ticker]['df']
            today_rows = df[df['날짜'] == date_str]
            if len(today_rows) == 0:
                continue

            today_idx = today_rows.index[0]
            if today_idx == 0:
                continue

            prev_close  = df.iloc[today_idx - 1]['종가']
            today_close = today_rows.iloc[0]['종가']

            if prev_close == 0:
                continue

            rates.append((today_close - prev_close) / prev_close * 100)

        if len(rates) >= 3:
            theme_rates[theme_name] = round(sum(rates) / len(rates), 2)

    return theme_rates


# =============================================
# 5. 종목이 속한 테마 중 +3% 이상인 테마 체크
# =============================================
def is_theme_rising(ticker_name, theme_dict, theme_rates):
    for theme_name, stock_names in theme_dict.items():
        if ticker_name in stock_names:
            rate = theme_rates.get(theme_name, 0)
            if rate >= THEME_CHANGE_RATE:
                return True, theme_name, rate
    return False, None, 0


# =============================================
# 6. 날짜별 거래대금 상위 50위 추출
# =============================================
def get_top_n_by_date(all_daily, date_str):
    records = []
    for ticker, item in all_daily.items():
        df  = item['df']
        row = df[df['날짜'] == date_str]
        if len(row) == 0:
            continue
        row = row.iloc[0]
        if '거래대금' not in row or pd.isna(row['거래대금']):
            continue
        records.append({
            '종목코드': ticker,
            '종목명'  : item['name'],
            '거래대금' : row['거래대금'],
            '종가'    : row['종가'],
            '거래량'  : row['거래량']
        })

    if len(records) == 0:
        return []

    df_rank = pd.DataFrame(records)
    df_rank = df_rank.sort_values('거래대금', ascending=False)
    return df_rank.head(TOP_N).to_dict('records')


# =============================================
# 7. 변동성 필터
# =============================================
def is_high_volatility(ticker, all_daily, date_str):
    if ticker not in all_daily:
        return False

    df        = all_daily[ticker]['df']
    df_before = df[df['날짜'] < date_str].tail(VOLATILITY_WINDOW)

    if len(df_before) < VOLATILITY_WINDOW:
        return False

    df_before = df_before.copy()
    df_before['고가상승률'] = (df_before['고가'] - df_before['종가']) / df_before['종가'] * 100
    count = (df_before['고가상승률'] >= VOLATILITY_RATE).sum()

    return count >= VOLATILITY_COUNT


# =============================================
# 8. 매수 조건 체크
# =============================================
def check_buy_condition(ticker, all_daily, date_str):
    if ticker not in all_daily:
        return False, 0, 0

    df         = all_daily[ticker]['df']
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
    change_rate  = (today_close - prev_close) / prev_close * 100

    if change_rate >= 29.0:
        return False, 0, 0

    condition1 = change_rate >= BUY_CHANGE_RATE
    condition2 = today_volume >= prev_volume * BUY_VOLUME_RATIO

    return condition1 and condition2, change_rate, prev_close


# =============================================
# 9. 분봉 데이터 로드
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
# 10. 매도 시뮬레이션
# 익절: +10%
# 손절: -5%
# 시간매도: 다음날 09:10
# =============================================
def simulate_trade(df_min, buy_date, buy_price, buy_time):
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

        # 다음날 09:10 시간매도
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
# 11. 백테스트 실행
# =============================================
def run_backtest(start_date, end_date):
    print(f"\n{'='*60}")
    print(f"📊 백테스트 시작: {start_date} ~ {end_date}")
    print(f"{'='*60}")

    all_daily      = load_all_daily()
    theme_dict     = load_theme_stocks()
    name_to_ticker = build_name_to_ticker(all_daily)

    sample_ticker   = list(all_daily.keys())[0]
    all_dates       = all_daily[sample_ticker]['df']['날짜'].tolist()
    trade_dates     = [d for d in all_dates if start_date <= d <= end_date]

    total_asset = TOTAL_ASSET
    results     = []

    for date_str in trade_dates:

        # 날짜별 테마 등락률 계산
        theme_rates = calc_theme_change_rate(
            theme_dict, all_daily, name_to_ticker, date_str
        )

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

            # 테마 등락률 +3% 이상 체크
            theme_ok, theme_name, theme_rate = is_theme_rising(
                name, theme_dict, theme_rates
            )
            if not theme_ok:
                continue

            # 분봉 데이터 로드
            df_min = load_min_data(ticker)
            if df_min is None:
                continue

            # 당일 분봉
            today_min = df_min[df_min['시간'].str[:8] == date_str].copy()

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

            # 매수 시점 찾기
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

            shares       = int((total_asset * BUY_RATIO) / buy_price)
            if shares == 0:
                continue

            buy_cost     = buy_price * shares * (1 + COMMISSION)
            sell_price, sell_reason, sell_time = simulate_trade(
                df_min, date_str, buy_price, buy_time
            )
            sell_revenue = sell_price * shares * (1 - COMMISSION - TAX)

            profit      = sell_revenue - buy_cost
            profit_rate = round((sell_price - buy_price) / buy_price * 100, 2)
            total_asset += profit

            buy_hhmm  = f"{buy_time[8:10]}:{buy_time[10:12]}"
            sell_date = sell_time[:8]
            sell_hhmm = f"{sell_time[8:10]}:{sell_time[10:12]}"

            print(f"[{date_str}] 🔵 {name} | 테마:{theme_name}({theme_rate}%) | "
                  f"등락률:{round(change_rate,1)}% | "
                  f"매수:{buy_price:,}원({buy_hhmm}) → "
                  f"매도:{sell_price:,}원({sell_date} {sell_hhmm}) | "
                  f"수익률:{profit_rate}% | {sell_reason}")

            results.append({
                '날짜'     : date_str,
                '종목명'   : name,
                '종목코드' : ticker,
                '테마명'   : theme_name,
                '테마등락률': theme_rate,
                '매수가'   : buy_price,
                '매수시간' : buy_hhmm,
                '매도가'   : sell_price,
                '매도시간' : f"{sell_date} {sell_hhmm}",
                '수익률'   : profit_rate,
                '수익금'   : int(profit),
                '매도사유' : sell_reason
            })

    # =============================================
    # 성과 지표 계산
    # =============================================
    strategy_return = round((total_asset - TOTAL_ASSET) / TOTAL_ASSET * 100, 2)
    start_dt        = datetime.strptime(start_date, "%Y%m%d")
    end_dt          = datetime.strptime(end_date, "%Y%m%d")
    years           = (end_dt - start_dt).days / 365.25

    cagr   = 0
    mdd    = 0
    sharpe = 0
    win    = 0

    if results:
        result_df = pd.DataFrame(results)
        win       = len(result_df[result_df['수익률'] > 0])

        cagr = round(((total_asset / TOTAL_ASSET) ** (1 / years) - 1) * 100, 2)

        asset_curve = [TOTAL_ASSET]
        running     = TOTAL_ASSET
        for _, r in result_df.iterrows():
            running += r['수익금']
            asset_curve.append(running)
        asset_series = np.array(asset_curve)
        peak         = np.maximum.accumulate(asset_series)
        drawdown     = (asset_series - peak) / peak * 100
        mdd          = round(drawdown.min(), 2)

        risk_free_rate  = 3.5
        returns         = result_df['수익률'].values
        avg_return      = np.mean(returns)
        std_return      = np.std(returns, ddof=1)
        trades_per_year = len(results) / years
        rf_per_trade    = risk_free_rate / trades_per_year

        if std_return != 0:
            sharpe = round((avg_return - rf_per_trade) / std_return, 2)

        filename = f"backtest_v5_result_{start_date}_{end_date}.xlsx"
        result_df.to_excel(filename, index=False)
        print(f"✅ 저장 완료: {filename}")

    # =============================================
    # 코스피 벤치마크
    # =============================================
    kospi_return_real = 0
    kospi_cagr        = 0
    kospi_mdd         = 0
    df_kospi          = None

    try:
        df_kospi = fdr.DataReader('KS11', start_date, end_date)
        if df_kospi is None or len(df_kospi) == 0:
            raise Exception("데이터 없음")
        df_kospi = df_kospi.sort_index()
    except Exception:
        try:
            headers = {'User-Agent': 'Mozilla/5.0'}
            rows    = []
            for page in range(1, 100):
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
                if df_p['날짜'].min() <= pd.to_datetime(start_date):
                    break
            df_kospi = pd.concat(rows).set_index('날짜').sort_index()
            df_kospi = df_kospi[
                (df_kospi.index >= pd.to_datetime(start_date)) &
                (df_kospi.index <= pd.to_datetime(end_date))
            ]
        except Exception as e2:
            print(f"코스피 데이터 로드 실패: {e2}")

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

    # =============================================
    # 최종 결과 출력
    # =============================================
    print(f"\n{'='*60}")
    print(f"📊 백테스트 결과: {start_date} ~ {end_date}")
    print(f"{'='*60}")
    print(f"{'지표':<15} {'전략':>12} {'코스피':>12}")
    print(f"{'-'*40}")
    print(f"{'초기자산':<15} {TOTAL_ASSET:>11,}원")
    print(f"{'최종자산':<15} {int(total_asset):>11,}원")
    print(f"{'총 수익률':<15} {strategy_return:>11}% {kospi_return_real:>11}%")
    print(f"{'CAGR':<15} {cagr:>11}% {kospi_cagr:>11}%")
    print(f"{'MDD':<15} {mdd:>11}% {kospi_mdd:>11}%")
    print(f"{'샤프지수':<15} {sharpe:>12} {'N/A':>12}")
    print(f"{'승률':<15} {round(win/len(results)*100,1) if results else 0:>11}% {'N/A':>12}")
    print(f"{'거래횟수':<15} {len(results):>11}회 {'N/A':>12}")
    print(f"")
    print(f"⚠️  Survivorship Bias 주의")
    print(f"   전략은 현재 상장 종목만 포함 → 수익률 소폭 과대평가 가능")

    if strategy_return > kospi_return_real:
        print(f"\n✅ 전략이 시장 대비 {round(strategy_return - kospi_return_real, 2)}% 초과수익!")
    else:
        print(f"\n❌ 전략이 시장 대비 {round(kospi_return_real - strategy_return, 2)}% 미달")


# =============================================
# 실행
# =============================================
run_backtest("20250304", "20251231")