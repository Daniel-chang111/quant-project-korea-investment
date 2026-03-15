import sys
from PyQt5.QtWidgets import QApplication
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop
import pandas as pd

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
# 분봉 데이터 요청 함수
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
            date  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "체결시간").strip()
            open_ = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "시가").strip()
            high  = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "고가").strip()
            low   = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "저가").strip()
            close = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "현재가").strip()
            volume = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, i, "거래량").strip()

            minute_data.append({
                '시간': date,
                '시가': abs(int(open_)),
                '고가': abs(int(high)),
                '저가': abs(int(low)),
                '종가': abs(int(close)),
                '거래량': abs(int(volume))
            })

        print(f"데이터 {count}개 수신 완료!")
        data_loop.quit()


kiwoom.OnReceiveTrData.connect(on_receive_chart_data)


def get_minute_data(ticker, timeframe=5):
    # ticker = 종목코드, timeframe = 몇 분봉 (5 = 5분봉)
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "종목코드", ticker)
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "틱범위", str(timeframe))
    kiwoom.dynamicCall("SetInputValue(QString,QString)", "수정주가구분", "1")
    kiwoom.dynamicCall(
        "CommRqData(QString,QString,int,QString)",
        "분봉차트", "opt10080", 0, "0101"
    )
    data_loop.exec_()


# =============================================
# 실행 - 삼성전자 5분봉 데이터 가져오기
# =============================================
get_minute_data("005930", timeframe=5)

df = pd.DataFrame(minute_data)
print(df.head(10))

# CSV로 저장
df.to_csv("samsung_5min.csv", index=False, encoding="utf-8-sig")
print("✅ CSV 저장 완료! → samsung_5min.csv")