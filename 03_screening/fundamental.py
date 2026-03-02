# 재무 데이터 수집 - 네이버 금융에서 PER, ROE 가져오기
import pandas as pd #표(데이터)다루는 도구
import requests #(웹 브라우저에서 데이터 다루기)
from io import StringIO #문자열을 파일처럼 변환
import time #시간 지연 가능

def get_naver_fundamental(page=1):
    # 네이버 금융 코스피 전종목 PER, ROE 페이지
    url = "https://finance.naver.com/sise/sise_market_sum.naver"
    params = {
        "sosok": "0",    # 0 = 코스피, 1 = 코스닥
        "page": page     # 페이지 번호
    }
    headers = {"User-Agent": "Mozilla/5.0"} #사이트에 일반 사용자인척 하기
    r = requests.get(url, params=params, headers=headers) #url에 접속해서 페이지 내용 가져오는 함수
    r.encoding = "euc-kr" #한글 깨지지 않게 보호
    tables = pd.read_html(StringIO(r.text)) #HTML 페이지에서 table=표 찾아서 리스트로 반환
    df = tables[1] #표중 2번째꺼 선택
    # NaN 행 제거 (빈 행)
    df = df.dropna(subset=['종목명']) #빈값이 있는 행은 삭제 함수
    return df


# =============================================
# 전체 페이지 수집 (1~18페이지)
# =============================================
all_data = []   # 모든 페이지 데이터 담을 빈 리스트

print("재무 데이터 수집 시작...")

for page in range(1, 19):   # 1페이지부터 18페이지까지 맨끝페이지를 알고있을때만 사용 아니면 아래 활용
    df = get_naver_fundamental(page=page)
    all_data.append(df)     # 리스트에 추가
    print(f"{page}페이지 수집 완료 ({len(df)}개 종목)")
    time.sleep(0.3)         # 서버 과부하 방지

# # ✅ 개선된 방식 - 빈 페이지가 나올 때까지 자동으로 반복
# page = 1

# while True:                              # 무한 반복 시작
#     df = get_naver_fundamental(page=page)

#     if len(df) == 0:                     # 가져온 데이터가 없으면 (빈 페이지면)
#         print("마지막 페이지 도달!")
#         break                            # 반복 중단!

#     all_data.append(df)
#     print(f"{page}페이지 수집 완료")
#     page += 1                            # 다음 페이지로
#     time.sleep(0.3)

# 모든 페이지 데이터를 하나의 표로 합치기
# pd.concat() = 여러 DataFrame을 위아래로 합치는 함수
result_df = pd.concat(all_data, ignore_index=True) #ignore: 합친 후 행번호를 새로 매김
print(f"\n총 {len(result_df)}개 종목 수집 완료!")


# =============================================
# 저평가 종목 필터링
# PER 10 이하 + ROE 10 이상
# =============================================

# PER, ROE를 숫자로 변환 (문자열로 되어있을 수 있어서)
result_df['PER'] = pd.to_numeric(result_df['PER'], errors='coerce') #errors: 변환 못하는 값 NaN처리
result_df['ROE'] = pd.to_numeric(result_df['ROE'], errors='coerce')

# 조건 필터링
filtered = result_df[
    (result_df['PER'] > 0) &       # PER이 양수인 것만
    (result_df['PER'] <= 10) &     # PER 10 이하
    (result_df['ROE'] >= 10)       # ROE 10% 이상
]

print(f"\n저평가 우량주 {len(filtered)}개 발견!")
print(filtered[['종목명', 'PER', 'ROE']].to_string(index=False))