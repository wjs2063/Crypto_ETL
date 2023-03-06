import requests
from datetime import datetime,timedelta
import pandas as pd
import time
from database import get_db
import asyncio
"""
time stamp : utc iso format
side : Buy,Sell
size : 수량
price : 가격

"""

"""
https://www.bitmex.com/api/explorer/#!/Trade/Trade_get
"""

# {'error': {'message': 'Rate limit exceeded, retry in 10 minutes.', 'name': 'RateLimitError'}}

def convert_unix_to_date(time):
    # 년 월 일 시 분 까지만 가져온다 (분단위)
    time /= 1000
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

def convert_iso_form(time):
    timestamp = int(datetime.fromisoformat(time[:-1]).timestamp() * 1000 + int(time[-4:-1]))
    return timestamp



def get_bitmex_data(startTime,endTime):
    bitmex_url = 'https://www.bitmex.com/api/v1/trade'
    param = {
        'symbol': 'XBTUSD',
        'count': 1000,
    "startTime":startTime,
    "endTime:":endTime
    }
    response = requests.get(bitmex_url ,params = param).json()
    # 시간순 정렬
    response.sort(key = lambda x: x["timestamp"])
    # API limited Error exception
    temp = []
    return response

def transform(df):
    df = df.astype({"trdMatchID":str,"price":float,"size":float,})
    df["timestamp"] = df["timestamp"].apply(lambda x: convert_iso_form(x))
    df["timestamp"] = df["timestamp"].apply(lambda x: convert_unix_to_date(x))
    return df

def aggregate(data:pd.DataFrame):
    data["bitmex_taker_buy_vol"] = data.apply(lambda x: x["price"] * x["size"] if x["side"] == "Buy" else 0,axis = 1)
    data["bitmex_taker_sell_vol"] = data.apply(lambda x: x["price"] * x["size"] if x["side"] == "Sell" else 0,axis = 1)
    data = data.groupby("time").agg({"bitmex_taker_buy_vol":sum,"bitmex_taker_sell_vol":sum}).reset_index()
    return data.to_dict(orient = "records")





async def insert_to_Db(data):
    async with get_db() as db:
        for x in data:
            x.update({"created_at" : datetime.now()})
            db.bitmex.insert_one(x)

last = datetime.utcnow()
start_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
df = pd.DataFrame(columns = ['timestamp', 'symbol', 'side', 'size', 'price', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional', 'trdType'])
print(start_date)
while True:
    try :
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
        if start_date != now:
            print(start_date,now)
            data = get_bitmex_data(start_date,now)
            if data:
                data = pd.DataFrame(data)
                data = transform(data)
                data = data[data["timestamp"] >= start_date]
                df = pd.concat([df,data],ignore_index = True)
                df = df.reset_index(drop = True)
            #데이터 이어붙히고
                #print(len(df))
                #df = df.drop_duplicates()
                #print(len(df))
                #df = df.reset_index(drop = True)
            df = df.drop_duplicates()
            df = df.sort_values(by = "timestamp").reset_index(drop = True)
            temp = df[(df["timestamp"] < now)]
            if len(temp) > 0 :
                index = temp.index[-1] + 1
                # data 분리
                data,df = df.iloc[:index,:],df.iloc[index:,:]
                data.rename(columns = {"timestamp":"time"},inplace = True)
                data = aggregate(data)
                print(data)
                asyncio.run(insert_to_Db(data))
                # data 만 처리
            start_date = now
        time.sleep(5)
    except KeyError as k:
        print(k)
        time.sleep(60 * 10)
    except Exception as e:
        print(e)
        time.sleep(60)

