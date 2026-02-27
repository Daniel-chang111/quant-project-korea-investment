from pykrx import stock
import matplotlib.pyplot as plt
import ta
# 삼성전자 주가 데이터 가져오기
df = stock.get_market_ohlcv("20240101", "20241231", "005930")
#이동평균선 계산
#roiing함수는 5, 20개씩 묶어서 계산하는 함수
df['MA5'] = df['종가'].rolling(5).mean()
df['MA20'] = df['종가'].rolling(20).mean()
#RSI 계산 (14일기준)
#ta는 Technical Analysis 기술적분석 라이브러리
df['RSI'] = ta.momentum.RSIIndicator(df['종가'], window=14).rsi()
#MACD
#객체만들기 코드
macd = ta.trend.MACD(df['종가'])
#순서대로 MACD선, 시그널, 히스토그램를 객체에서 꺼내옴
df['MACD'] = macd.macd()
df['MACD_signal'] = macd.macd_signal()
df['MACD_diff'] = macd.macd_diff()
#볼린저밴드
bb = ta.volatility.BollingerBands(df['종가'], window=20)
df['BB_upper'] = bb.bollinger_hband()
df['BB_middle'] = bb.bollinger_mavg()
df['BB_lower'] = bb.bollinger_lband()
#차트 2~3개 나눠서 그리기
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(14, 12), sharex=True)
#위 차트 -> 주가 + 이동평균선
ax1.plot(df.index, df['종가'], label='종가', color='black', linewidth=1)
ax1.plot(df.index, df['MA5'], label='MA5', color='blue', linewidth=1)
ax1.plot(df.index, df['MA20'], label='MA20', color='red', linewidth=1)
ax1.plot(df.index, df['BB_upper'], label='BB상단', color='green', linestyle='--', linewidth='1')
ax1.plot(df.index, df['BB_lower'], label='BB하단', color='green', linestyle='--', linewidth='1')
#볼린저밴드 사이 색채우기
ax1.fill_between(df.index, df['BB_upper'], df['BB_lower'], alpha=0.1, color='green')
ax1.set_title('삼성전자 2024년 주가')
#legend=범례, 범례 추가함수
ax1.legend()
#아래 차트 -> RSI
ax2.plot(df.index, df['RSI'], label='RSI', color='purple', linewidth=1)
ax2.axhline(y=70, color='red', linestyle='--', linewidth=1)
ax2.axhline(y=30, color='blue', linestyle='--',linewidth=1)
ax2.set_title('RSI')
ax2.legend()
#아래 차트 -> MACD
ax3.plot(df.index, df['MACD'], label='MACD', color='blue', linewidth=1)
ax3.plot(df.index, df['MACD_signal'], label='Signal', color='red', linewidth=1)
ax3.bar(df.index, df['MACD_diff'], label='Histogram', color='gray', alpha=0.5)
ax3.set_title('MACD')
ax3.legend()

plt.tight_layout()
plt.show()

