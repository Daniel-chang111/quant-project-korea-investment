# 재무 데이터 수집 - 네이버 금융에서 PER, ROE 가져오기
import pandas as pd
import requests
from io import StringIO
import time

def get_naver_fundamental(page=1):
    # 네이버 금융 코스피 전종목 PER, ROE 페이지
    url = "https://finance.naver.com/sise/sise_market_sum.naver"
    params = {
        "sosok": "0",    # 0 = 코스피, 1 = 코스닥
        "page": page     # 페이지 번호
    }
    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, params=params, headers=headers)
    r.encoding = "euc-kr"
    tables = pd.read_html(StringIO(r.text))
    df = tables[1]
    # NaN 행 제거 (빈 행)
    df = df.dropna(subset=['종목명'])
    return df


# =============================================
# 전체 페이지 수집 (1~18페이지)
# =============================================
all_data = []   # 모든 페이지 데이터 담을 빈 리스트

print("재무 데이터 수집 시작...")

for page in range(1, 19):   # 1페이지부터 18페이지까지
    df = get_naver_fundamental(page=page)
    all_data.append(df)     # 리스트에 추가
    print(f"{page}페이지 수집 완료 ({len(df)}개 종목)")
    time.sleep(0.3)         # 서버 과부하 방지

# 모든 페이지 데이터를 하나의 표로 합치기
# pd.concat() = 여러 DataFrame을 위아래로 합치는 함수
result_df = pd.concat(all_data, ignore_index=True)
print(f"\n총 {len(result_df)}개 종목 수집 완료!")


# =============================================
# 저평가 종목 필터링
# PER 10 이하 + ROE 10 이상
# =============================================

# PER, ROE를 숫자로 변환 (문자열로 되어있을 수 있어서)
result_df['PER'] = pd.to_numeric(result_df['PER'], errors='coerce')
result_df['ROE'] = pd.to_numeric(result_df['ROE'], errors='coerce')

# 조건 필터링
filtered = result_df[
    (result_df['PER'] > 0) &       # PER이 양수인 것만
    (result_df['PER'] <= 10) &     # PER 10 이하
    (result_df['ROE'] >= 10)       # ROE 10% 이상
]

print(f"\n저평가 우량주 {len(filtered)}개 발견!")
print(filtered[['종목명', 'PER', 'ROE']].to_string(index=False))