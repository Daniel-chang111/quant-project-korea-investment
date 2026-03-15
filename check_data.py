import pandas as pd

df = pd.read_csv('data_backtest/min/032350_롯데관광개발.csv')
df['시간'] = df['시간'].astype(str)
df = df.sort_values('시간')
df = df[df['시간'].str[:8] == '20251118']
print(df[['시간','시가','고가','저가','종가']].to_string())