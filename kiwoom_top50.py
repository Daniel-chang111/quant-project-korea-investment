import sys
import os
from PyQt5.QtWidgets import QApplication
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
import pandas as pd
import time
from datetime import datetime

app = QApplication(sys.argv)
kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

# =============================================
# 로그인
# =============================================
login_loop = QEventLoop()

def on_login(err_code):
    if err_code == 0:
        print("✅ 로그인 성공!")
    else:
        print(f"❌ 로그인 실패: {err_code}")
    login_loop.quit()

kiwoom.OnEventConnect.connect(on_login)
kiwoom.dynamicCall("CommConnect()")
login_loop.exec_()


# =============================================
# 거래대금 상위 50위 종목 조회
# =============================================
tr_loop = QEventLoop()
top50_tickers = []

def on_receive_tr_data(screen_no, rqname, trcode, record_name, prev_next):
    global top50_tickers

    if rqname == "거래대금상위":
        count = kiwoom.dynamicCall(
            "GetRepeatCnt(QString, QString)", trcode, rqname
        )
        for i in range(count):
            ticker = kiwoom.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode, rqname, i, "종목코드"
            ).strip()
            name = kiwoom.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode, rqname, i, "종목명"
            ).strip()
            volume_amount = kiwoom.dynamicCall(
                "GetCommData(QString,QString,int,QString)",
                trcode, rqname, i, "거래대금"
            ).strip()

            if ticker:
                top50_tickers.append({
                    '종목코드': ticker,
                    '종목명': name,
                    '거래대금': volume_amount
                })

        print(f"거래대금 상위 {len(top50_tickers)}개 종목 수신 완료!")
        tr_loop.quit()

kiwoom.OnReceiveTrData.connect(on_receive_tr_data)

def get_top50():
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "시장구분", "0")  # 0=코스피
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "정렬구분", "1")  # 1=거래대금
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "관리종목포함", "0")
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "신용구분", "0")
    kiwoom.dynamicCall(
        "CommRqData(QString,QString,int,QString)",
        "거래대금상위", "opt10030", 0, "0201"
    )
    tr_loop.exec_()


# =============================================
# 분봉 데이터 수집
# =============================================
data_loop = QEventLoop()
minute_data = []

def on_receive_chart_data(screen_no, rqname, trcode, record_name, prev_next):
    global minute_data

    if rqname == "분봉차트":
        count = kiwoom.dynamicCall(
            "GetRepeatCnt(QString, QString)", trcode, rqname
        )
        for i in range(count):
            date   = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "체결시간").strip()
            open_  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "시가").strip()
            high   = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "고가").strip()
            low    = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "저가").strip()
            close  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "현재가").strip()
            volume = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "거래량").strip()

            minute_data.append({
                '시간': date,
                '시가': abs(int(open_)),
                '고가': abs(int(high)),
                '저가': abs(int(low)),
                '종가': abs(int(close)),
                '거래량': abs(int(volume))
            })

        data_loop.quit()

kiwoom.OnReceiveTrData.connect(on_receive_chart_data)

def get_minute_data(ticker, timeframe=5):
    global minute_data
    minute_data = []  # 초기화
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "종목코드", ticker)
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "틱범위", str(timeframe))
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")
    kiwoom.dynamicCall(
        "CommRqData(QString,QString,int,QString)",
        "분봉차트", "opt10080", 0, "0101"
    )
    data_loop.exec_()
    return pd.DataFrame(minute_data)


# =============================================
# 실행 코드
# =============================================

# ETF, ETN 제외 키워드
exclude_keywords = ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'HANARO',
                    'ARIRANG', 'KOSEF', 'FOCUS', 'SOL', 'ACE', 'RISE']

# 1단계: 거래대금 상위 50위 조회
print("거래대금 상위 종목 조회 중...")
get_top50()

# 상위 50개만 자르기
top50 = top50_tickers[:50]
today = datetime.today().strftime("%Y%m%d")
print(f"총 {len(top50)}개 종목 수집 시작...")

# 2단계: 각 종목 5분봉 데이터 수집
for i, item in enumerate(top50):
    ticker = item['종목코드']
    name = item['종목명']

    # ETF/ETN 제외
    if any(keyword in name.upper() for keyword in exclude_keywords):
        print(f"  → ETF/ETN 제외: {name}")
        continue

    # 이미 수집된 파일 있으면 건너뛰기
    filename = f"data/{ticker}_{name}_{today}.csv"
    if os.path.exists(filename):
        print(f"  → 이미 수집됨, 건너뜀: {name}")
        continue

    print(f"[{i+1}/50] {name} ({ticker}) 수집 중...")
    df = get_minute_data(ticker, timeframe=5)

    if len(df) > 0:
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"  → {len(df)}개 저장 완료!")
    else:
        print(f"  → 데이터 없음")

    time.sleep(0.5)  # API 호출 간격 (초당 5회 제한)

print(f"\n✅ 전체 수집 완료!")