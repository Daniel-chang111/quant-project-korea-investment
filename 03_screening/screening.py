# pykrx 대신 KIND + FinanceDataReader 조합으로 스크리닝
import pandas as pd #데이터를 표 형태로 다루는 도구
import requests #인터넷에서 데이터 가져오는 도구
from io import StringIO #텍스트를 파일처럼 읽게 해주는 도구
import time #시간관련 도구
import ta #기술적 지표 계산 도구
import FinanceDataReader as fdr #


# 코스피 전 종목 리스트 가져오는 함수
def get_kospi_tickers_from_kind(timeout=15): #15초 안에 응답 없으면 포기
    url = "https://kind.krx.co.kr/corpgeneral/corpList.do" # 데이터 가져올 주소
    #params = 주소에 뒤에 붙는 검색값 (딕셔너리 형태 = {메소드:다운로드, 마케타입: 주식시장})
    params = {"method": "download", "marketType": "stockMkt"}
    # headers = 봇임을 속이기 위해 브라우저인척(안하면 사이트가 차단할 수 있어요)
    headers = {"User-Agent": "Mozilla/5.0", "Referer": "https://kind.krx.co.kr/"}
    #인터넷 데이터 가져오기
    r = requests.get(url, params=params, headers=headers, timeout=timeout)
    r.raise_for_status()
    #컴퓨터는 숫자로만 읽을 수 있어서 데이터를 한국어로 인코딩
    html = r.content.decode("euc-kr", errors="ignore")
    #페이지에 표가 있으면 읽어서 데이터프레임으로 변환
    df = pd.read_html(StringIO(html))[0]
    #순서대로 표에서 종목코드 열만 가져오고, 숫자로 된 코드는 문자열로 변환, 앞뒤빈칸 제거
    code = df["종목코드"].astype(str).str.strip()
    #티커 필터링하는 코드, 패터엔 맞는 것만 골라내는 함수, 6자리 숫자로만 이루어진 것만 통과
    tickers = code[code.str.match(r"^\d{6}$")].tolist()
    return tickers, df


# RSI 계산 함수
def get_rsi(ticker, start="20250101", end="20250221"):
    try:
        df = fdr.DataReader(ticker, start, end)
        #RSI는 14일치 자료가 필요한데 만약 20개 미만이면 넘어가
        if len(df) < 20:
            return None
            #close:종가값만 추출, window:rsi계산 일
        rsi = ta.momentum.RSIIndicator(df['Close'], window=14).rsi()
        #-1: 맨뒤에서 첫번째로 가장 최신rsi값 지정, round:반올림, 2: 소수둘째
        return round(rsi.iloc[-1], 2)
        #try로 실행하고 except:만약 오류나면 None반환하고 다음 종목으로 넘어가
    except:
        return None


# 종목 리스트 가져오기
tickers, df = get_kospi_tickers_from_kind()
print(f"코스피 전체 종목 수: {len(tickers)}개")

result = []
print("스크리닝 시작...")
#enumerate: 순서번호와 값을 동시에 가져옴
for i, ticker in enumerate(tickers):      # [:50] 제거 → 전체 종목
    rsi = get_rsi(ticker)
    #not None:계산이 됐을때나 rsi가 35이하일때
    if rsi is not None and rsi <= 35:      # 35 이하로 다시 조건 강화
# 종목코드 열 가져오기
# → 문자열로 변환 astype
# → 6자리로 맞추기 zfill
# → ticker랑 같은 행 찾기 (True/False): ==ticker 
# → True인 행만 필터링: df['종목코드]
# → 회사명 열만 가져오기: df['회사명']
# → 값만 추출: .values
# → ["삼성전자"]
        name = df[df['종목코드'].astype(str).str.zfill(6) == ticker]['회사명'].values
        name = name[0] if len(name) > 0 else ticker
        result.append({'종목코드': ticker, '종목명': name, 'RSI': rsi})
        print(f"✅ {name} ({ticker}) RSI: {rsi}")
    time.sleep(0.3)

print(f"\n총 {len(result)}개 종목 발견!")
