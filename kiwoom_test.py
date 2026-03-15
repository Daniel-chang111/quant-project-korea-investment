import sys
from PyQt5.QtWidgets import QApplication
from PyQt5.QAxContainer import QAxWidget
from PyQt5.QtCore import QEventLoop

app = QApplication(sys.argv)
kiwoom = QAxWidget("KHOPENAPI.KHOpenAPICtrl.1")

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

# opt10030 파라미터 확인
tr_loop = QEventLoop()

def on_receive_tr_data(screen_no, rqname, trcode, record_name, prev_next):
    if rqname == "거래대금상위":
        count = kiwoom.dynamicCall("GetRepeatCnt(QString,QString)", trcode, rqname)
        print(f"수신 종목 수: {count}")

        # 첫 번째 종목 데이터 확인
        name = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, 0, "종목명").strip()
        date = kiwoom.dynamicCall("GetCommData(QString,QString,int,QString)", trcode, rqname, 0, "일자").strip()
        print(f"종목명: {name}")
        print(f"일자: {date}")
        tr_loop.quit()

kiwoom.OnReceiveTrData.connect(on_receive_tr_data)

# 과거 날짜로 조회 테스트
kiwoom.dynamicCall("SetInputValue(QString,QString)", "시장구분", "0")
kiwoom.dynamicCall("SetInputValue(QString,QString)", "정렬구분", "1")
kiwoom.dynamicCall("SetInputValue(QString,QString)", "관리종목포함", "0")
kiwoom.dynamicCall("SetInputValue(QString,QString)", "신용구분", "0")
kiwoom.dynamicCall("SetInputValue(QString,QString)", "일자", "20260220")  # 과거 날짜 테스트
kiwoom.dynamicCall("CommRqData(QString,QString,int,QString)", "거래대금상위", "opt10030", 0, "0201")
tr_loop.exec_()