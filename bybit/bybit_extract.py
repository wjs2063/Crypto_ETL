import requests
from datetime import datetime,timedelta
import pandas as pd
import time
from database import get_db
import asyncio

def convert_unix_to_date(time):
    # 년 월 일 시 분 까지만 가져온다 (분단위)
    time /= 1000
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

def convert_iso_form(time):
    timestamp = int(datetime.fromisoformat(time[:-1]).timestamp() * 1000 + int(time[-4:-1]))
    return timestamp



def bybit():
    bybit_url = "https://api-testnet.bybit.com/v2/public/trading-records"
    param = {
        "symbol" : "BTCUSD",
        "limit" : 1000
    }
    response = requests.get(bybit_url ,params = param).json()
    if not response.get("result"):return []
    response = response["result"]
    # 시간순 정렬
    response.sort(key = lambda x: x["time"])
    #시간변환
    return response

def transform(df):
    data = df.astype({"id":int,"price":float,"qty":float,})
    df["time"] = df["time"].apply(lambda x: convert_iso_form(x))
    df["time"] = df["time"].apply(lambda x: convert_unix_to_date(x))
    return df

async def insert_to_Db(data):
    async with get_db() as db:
        for x in data:
            x.update({"created_at" : datetime.now()})
            db.bybit.insert_one(x)




def aggregate(data:pd.DataFrame):
    data["bybit_taker_buy_vol"] = data.apply(lambda x: x["price"] * x["qty"] if x["side"] == "Buy" else 0,axis = 1)
    data["bybit_taker_sell_vol"] = data.apply(lambda x: x["price"] * x["qty"] if x["side"] == "Sell" else 0,axis = 1)
    data = data.groupby("time").agg({"bybit_taker_buy_vol":sum,"bybit_taker_sell_vol":sum}).reset_index()
    return data.to_dict(orient = "records")


df = pd.DataFrame(columns = ["id","symbol","price","qty","side","time"])
start_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
print(start_date)
while True:
    try :
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
        data = bybit()
        if data:
            data = pd.DataFrame(data)
            data = transform(data)
            data = data[data["time"] >= start_date]
            df = pd.concat([df,data],ignore_index = True)
            df = df.reset_index(drop = True)
        #데이터 이어붙히고
        if start_date != now:
            if len(df) > 0 :
                df = df.drop_duplicates()
                df = df.reset_index(drop = True)
                index = df[(df["time"] < now)].index[-1] + 1
                # data 분리
                data,df = df.iloc[:index,:],df.iloc[index :,:]
                data = aggregate(data)
                print(data)
                asyncio.run(insert_to_Db(data))
            # data 만 처리
            start_date = now
        time.sleep(10)
    except Exception as e :
        print(e)


