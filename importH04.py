# 載入 #

import datetime
import json
import pymssql
import requests
import sys

# 載入connection模組
from GetSettings import Connection

from GetSettings import Model

#   DB 設定。
_Server = ""
_User = ""
_Password = ""
_Database = ""

#--- 00 取得設定參數 ---

#   設定檔案路徑與名稱。
FilePath = 'AppSettings.json'

#   取得設定。
try:
    #   取得 DB參數設定。
    ConnectionInfo = Connection.GetDatabaseConnectionInfo(FilePath)

    _Server = ConnectionInfo.Server
    _User = ConnectionInfo.User
    _Password = ConnectionInfo.Password
    _Database = ConnectionInfo.Database

except FileNotFoundError as e:
    print(f"錯誤：{e}")
except ValueError as e:
    print(f"錯誤：{e}")
#--- End 00 取得設定參數 ---

#   模式測試
ModelInfo = Model.GetModel(FilePath)
print(ModelInfo.Type)

#--- 01 取得當日資料 ---
#   API 位置。
OpenDataLink = "https://www.twse.com.tw/exchangeReport/STOCK_DAY_ALL?response=json"

#   取得期交所資料。
Response = requests.get(OpenDataLink)

#   解析。
data = json.loads(Response.text)
#   檢查 API回傳值。
if data["stat"] != "OK":
    sys.exit()

HistoryData =  data["data"]
#--- End 01 取得當日資料 ---

#--- 02 匯入 當日日成交資訊---
#   建立 Connection物件。
conn = pymssql.connect(
    server = _Server,
    user = _User,
    password = _Password,
    database = _Database,
    as_dict = True
)

#   建立 SQL 指令。
Sql = """Insert Into DAILY_PRICE(TRADE_DATE, STOCK_ID, SYMBOL, TRADE_VOLUME, TRADE_VALUE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, CLOSE_PRICE, CHANGE, [TRANSACTION]) 
         Values(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"""

#   建立 Cursor物件。
with conn.cursor() as cursor:
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    
    for HistoryInfo in HistoryData:
        #   匯入。
        cursor.execute(
            Sql, (
                now, 
                HistoryInfo[0],
                HistoryInfo[1],
                int(HistoryInfo[2].replace(",", "")),
                int(HistoryInfo[3].replace(",", "")),
                HistoryInfo[4].replace(",", ""),
                HistoryInfo[5].replace(",", ""),
                HistoryInfo[6].replace(",", ""),
                HistoryInfo[7].replace(",", ""),
                HistoryInfo[8].replace("X", ""),
                int(HistoryInfo[9].replace(",", ""))
                )
            )
        conn.commit()
#--- End 02 匯入 當日日成交資訊---