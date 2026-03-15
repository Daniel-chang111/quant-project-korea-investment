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
# 일봉 데이터 수집
# =============================================
data_loop = QEventLoop()
daily_data = []

def on_receive_daily_data(screen_no, rqname, trcode, record_name, prev_next):
    global daily_data

    if rqname == "일봉차트":
        count = kiwoom.dynamicCall(
            "GetRepeatCnt(QString, QString)", trcode, rqname
        )
        for i in range(count):
            date   = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "일자").strip()
            open_  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "시가").strip()
            high   = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "고가").strip()
            low    = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "저가").strip()
            close  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "현재가").strip()
            volume = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "거래량").strip()

            if date:
                daily_data.append({
                    '날짜': date,
                    '시가': abs(int(open_)),
                    '고가': abs(int(high)),
                    '저가': abs(int(low)),
                    '종가': abs(int(close)),
                    '거래량': abs(int(volume))
                })

        data_loop.quit()

kiwoom.OnReceiveTrData.connect(on_receive_daily_data)

def get_daily_data(ticker):
    global daily_data
    daily_data = []  # 초기화
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "종목코드", ticker)
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "기준일자", datetime.today().strftime("%Y%m%d"))
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")
    kiwoom.dynamicCall(
        "CommRqData(QString,QString,int,QString)",
        "일봉차트", "opt10081", 0, "0301"
    )
    data_loop.exec_()
    return pd.DataFrame(daily_data)


# =============================================
# 실행 코드
# =============================================

# data 폴더에서 수집된 종목코드 읽기
data_dir = "data"
daily_dir = "data_daily"

# data_daily 폴더 없으면 생성
if not os.path.exists(daily_dir):
    os.makedirs(daily_dir)

# 분봉 데이터에서 종목코드 목록 추출
files = os.listdir(data_dir)
tickers = []
for f in files:
    if f.endswith(".csv"):
        parts = f.replace(".csv", "").split("_")
        ticker = parts[0]
        name = parts[1] if len(parts) > 1 else ticker
        tickers.append({'종목코드': ticker, '종목명': name})

print(f"총 {len(tickers)}개 종목 일봉 데이터 수집 시작...")

today = datetime.today().strftime("%Y%m%d")

# ETF, ETN 제외 키워드
exclude_keywords = ['ETF', 'ETN', 'KODEX', 'TIGER', 'KBSTAR', 'HANARO',
                    'ARIRANG', 'KOSEF', 'FOCUS', 'SOL', 'ACE', 'RISE']

for i, item in enumerate(tickers):
    ticker = item['종목코드']
    name = item['종목명']

    # ETF/ETN 제외
    if any(keyword in name.upper() for keyword in exclude_keywords):
        print(f"  → ETF/ETN 제외: {name}")
        continue

    # 이미 수집된 파일 있으면 건너뛰기
    filename = f"{daily_dir}/{ticker}_{name}_{today}.csv"
    if os.path.exists(filename):
        print(f"  → 이미 수집됨, 건너뜀: {name}")
        continue

    print(f"[{i+1}/{len(tickers)}] {name} ({ticker}) 일봉 수집 중...")
    df = get_daily_data(ticker)

    if len(df) > 0:
        df.to_csv(filename, index=False, encoding="utf-8-sig")
        print(f"  → {len(df)}개 저장 완료!")
    else:
        print(f"  → 데이터 없음")

    time.sleep(0.5)

print(f"\n✅ 일봉 데이터 수집 완료!")