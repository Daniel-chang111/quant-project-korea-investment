from pykrx import stock

# 오늘 날짜 기준 코스피 전 종목 리스트 가져오기
tickers = stock.get_market_ticker_list(market="KOSPI")

# 종목 수 출력
print(f"코스피 전체 종목 수: {len(tickers)}개")

# 앞에 10개만 출력
print(tickers[:10])