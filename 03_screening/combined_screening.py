# RSI + 재무 복합 조건 스크리닝
# 조건: RSI 45 이하 + PER 10 이하 + ROE 10 이상 + 거래량 1.5배 이상

# =============================================
# 도구 불러오기
# =============================================
import pandas as pd          # 데이터를 표 형태로 다루는 도구
import requests              # 인터넷에서 데이터 가져오는 도구
from io import StringIO      # 텍스트를 파일처럼 읽게 해주는 도구
import time                  # 시간 관련 도구 (기다리기)
import ta                    # 기술적 지표 계산 도구 (RSI 등)
import FinanceDataReader as fdr  # 주식 데이터 가져오는 도구
from datetime import datetime    # 날짜 관련 도구


# =============================================
# 1. 네이버 금융에서 PER, ROE 데이터 가져오기
# =============================================
def get_naver_fundamental():
    all_data = []  # 모든 페이지 데이터 담을 빈 바구니
    url = "https://finance.naver.com/sise/sise_market_sum.naver"  # 네이버 금융 주소
    headers = {"User-Agent": "Mozilla/5.0"}  # 브라우저인 척 하는 정보

    print("재무 데이터 수집 중...")

    # 1페이지부터 18페이지까지 반복 (한 페이지에 50개 종목)
    for page in range(1, 19):
        params = {"sosok": "0", "page": page}  # sosok=0: 코스피, page: 페이지 번호
        r = requests.get(url, params=params, headers=headers)  # 해당 페이지 접속
        r.encoding = "euc-kr"  # 한국어 인코딩 설정
        tables = pd.read_html(StringIO(r.text))  # HTML에서 모든 표 읽기
        df = tables[1]   # 두 번째 표가 종목 데이터
        df = df.dropna(subset=['종목명'])  # 종목명이 비어있는 행 제거
        all_data.append(df)  # 바구니에 이 페이지 데이터 추가
        time.sleep(0.3)  # 0.3초 대기 (서버 과부하 방지)

    # pd.concat() = 18개 페이지 데이터를 위아래로 합치기
    # ignore_index=True = 행 번호를 0부터 새로 매기기
    result = pd.concat(all_data, ignore_index=True)

    # pd.to_numeric() = 문자열로 된 숫자를 실제 숫자로 변환
    # errors='coerce' = 변환 안 되는 값은 NaN으로 처리
    result['PER'] = pd.to_numeric(result['PER'], errors='coerce')
    result['ROE'] = pd.to_numeric(result['ROE'], errors='coerce')

    print(f"재무 데이터 {len(result)}개 수집 완료!")
    return result  # 완성된 표 반환


# =============================================
# 2. KIND에서 코스피 종목코드 + 회사명 가져오기
# =============================================
def get_kind_df():
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do"  # KIND 사이트 주소
    params = {"method": "download", "marketType": "stockMkt"}  # 코스피 다운로드 조건
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}  # 브라우저인 척
    r = requests.get(url, params=params, headers=headers, timeout=15)  # 15초 안에 응답 없으면 포기
    html = r.content.decode("euc-kr", errors="ignore")  # 한국어로 디코딩
    kind_df = pd.read_html(StringIO(html))[0]  # HTML에서 첫 번째 표 읽기

    # str.zfill(6) = 6자리 미만이면 앞에 0 채우기 (예: "5930" → "005930")
    kind_df['종목코드'] = kind_df['종목코드'].astype(str).str.zfill(6)
    return kind_df  # 회사명 + 종목코드 표 반환


# =============================================
# 3. RSI + 거래량 계산 함수
# =============================================
def get_rsi_volume(ticker, start="20260101", end="20260303"):
    # try = 일단 실행해보고 오류나면 except로 넘어가
    try:
        df = fdr.DataReader(ticker, start, end)  # 해당 종목 주가 데이터 가져오기

        # RSI 계산에 최소 20개 데이터 필요 → 부족하면 None 반환
        if len(df) < 20:
            return None, None

        # RSI 계산 (14일 기준)
        rsi = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        rsi_value = round(rsi.iloc[-1], 2)  # 가장 최근 RSI값, 소수점 2자리

        # 거래량 계산
        volume_mean = df['Volume'].rolling(20).mean().iloc[-1]  # 20일 평균 거래량
        volume_last = df['Volume'].iloc[-1]  # 가장 최근 거래량

        # 평균 거래량이 0이거나 비어있으면 계산 불가 → None 반환
        if volume_mean == 0 or pd.isna(volume_mean):
            return None, None

        # 거래량 비율 = 오늘 거래량 / 20일 평균 거래량
        # 예) 오늘 100만 / 평균 50만 = 2.0배
        volume_ratio = round(volume_last / volume_mean, 2)

        return rsi_value, volume_ratio  # RSI, 거래량비율 동시에 반환

    except:
        return None, None  # 오류나면 None 반환하고 다음 종목으로 넘어가


# =============================================
# 실행 코드
# =============================================

# 1단계: 네이버 금융에서 재무 데이터 가져오기
fund_df = get_naver_fundamental()

# 2단계: PER, ROE 조건으로 필터링
# & = AND (모든 조건을 동시에 만족해야 통과)
filtered_fund = fund_df[
    (fund_df['PER'] > 0) &    # PER이 양수인 것만 (음수 제거)
    (fund_df['PER'] <= 10) &  # PER 10 이하 (저평가)
    (fund_df['ROE'] >= 10)    # ROE 10% 이상 (수익성 좋음)
]
print(f"재무 조건 통과 종목: {len(filtered_fund)}개")

# 3단계: KIND에서 회사명 + 종목코드 표 가져오기
kind_df = get_kind_df()

# 4단계: 재무 통과 종목만 RSI 계산
result = []  # 최종 결과 담을 빈 바구니
print("RSI 스크리닝 시작...")

# iterrows() = DataFrame을 한 행씩 꺼내오는 함수
# i = 행 번호, row = 그 행의 데이터
for i, row in filtered_fund.iterrows():
    name = row['종목명']  # 네이버에서 가져온 종목명

    # KIND 표에서 회사명이 일치하는 행의 종목코드 찾기
    matched = kind_df[kind_df['회사명'] == name]['종목코드'].values
    if len(matched) == 0:  # 종목코드를 못 찾으면
        print(f"❌ 종목코드 못 찾음: {name}")
        continue  # 이 종목 건너뛰고 다음으로
    ticker = matched[0]  # 찾은 종목코드 첫 번째 값 사용

    # RSI + 거래량 계산 함수 호출
    # 두 가지 값을 동시에 받음
    rsi, volume_ratio = get_rsi_volume(ticker)

    # 세 가지 조건 동시에 체크
    if rsi is not None and rsi <= 45 and volume_ratio >= 1.5:
        # 조건 통과하면 result 바구니에 딕셔너리 형태로 추가
        result.append({
            '종목명': name,
            '종목코드': ticker,
            'RSI': rsi,
            '거래량비율': volume_ratio,
            'PER': row['PER'],
            'ROE': row['ROE']
        })
        print(f"✅ {name} RSI: {rsi} PER: {row['PER']} ROE: {row['ROE']}")
    time.sleep(0.3)  # 0.3초 대기 (서버 과부하 방지)

# 5단계: 결과 출력 및 저장
print(f"\n🎯 복합 조건 통과 종목: {len(result)}개")

# result 리스트 → DataFrame(표)으로 변환
result_df = pd.DataFrame(result)

# to_string(index=False) = 행 번호 없이 표 출력
print(result_df.to_string(index=False))

# 오늘 날짜 (파일명에 사용)
today = datetime.today().strftime("%Y%m%d")  # 예) 20260303

# 엑셀로 저장
result_df.to_excel(f"03_screening/복합스크리닝_{today}.xlsx", index=False)
print(f"✅ 엑셀 저장 완료!")