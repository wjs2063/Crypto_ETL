import requests
from datetime import datetime,timedelta
import pandas as pd
#from database import get_db
import time


# qty : 매수/매도량 (주문량)
# quoteQty :주문 암호화폐 금액
# volume : 거래량
# qty * price ~= quoteQty

"""
isBuyerMaker -> True -> + Taker sell Volume ( Maker 가 Buyer 라면 Taker 가 판매자라는뜻 )
isBuyerMaker -> False -> + Taker buy Volume ( Maker 가 Buyer 가아니면 Taker 가 구매자 라는뜻)

"""

# start_time <= x <= end_time 인데 limit 가 1000개로 걸려있다.
"""
첫호출에 마지막 시간을 last 라고 하자 last < end_time 이면 계속 호출한다.
start_time = last , end_time = end_time  으로 둔다.
"""


# Binance
def convert_unix_to_date(time):
    time /= 1000
    # 년 월 일 시 분 까지만 가져온다 (분단위)
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

def binance():
    try :
        binance_url = "https://fapi.binance.com/fapi/v1/trades"

        data = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
        binance_param = {
            "symbol": "BTCUSDT",
        }
        response = requests.get(binance_url,params = binance_param).json()
        df = pd.DataFrame(response)
        #for item in response:
            #item["time"] = convert_unix_to_date(item["time"])
        data = pd.concat([data,df],ignore_index = True,axis = 0)
        data = data.astype({"id":int,"price":float,"qty":float,"quoteQty":float})
        data["time"] = data.apply(lambda x: convert_unix_to_date(x["time"]),axis = 1)
        data["binance_taker_buy_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] == False else 0,axis = 1)
        data["binance_taker_sell_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] else 0,axis = 1)
        data = data.groupby("time").agg({"binance_taker_buy_vol":sum,"binance_taker_sell_vol":sum}).reset_index()
        return data.to_dict(orient = "records")
    except Exception as e :
        print(e)
    return []




from pymongo import MongoClient
MONGO_DETAILS = "mongodb://172.30.1.56:45000/test"
from database import get_db

#db = MongoClient(MONGO_DETAILS)
#x = binance()
#db.local.data.insert_one(x[0])
import asyncio
async def Insert_to_Db():
    async with get_db() as db:
        x = binance()
        print(x)
        db.data.insert_one(x[0])
while True:
    asyncio.run(Insert_to_Db())
    time.sleep(1)
"""

while True:
    x = binance(start_time,end_time - 1)
    print(dict(x))
    #db.local.data.insert_one(dict(x))
    time.sleep(1)

"""


