#-*- coding:utf-8 -*-
import requests
from datetime import datetime
import pandas as pd
from database import get_db
import time
import asyncio
from typing import List,Optional
import logging
logging.basicConfig(filename = 'info.log', encoding = 'utf-8', level = logging.DEBUG)
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

def get_binance_data(last_id: int) -> List[dict]:
    try :
        binance_url = "https://fapi.binance.com/fapi/v1/trades"
        data = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
        binance_param = {
            "symbol": "BTCUSDT",
        }
        response = requests.get(binance_url,params = binance_param).json()
        # 가져온 데이터들중 last_id 보다 큰것만 계속 넣는다
        data = []
        for item in response:
            if item["id"] <= last_id : continue
            last_id = item["id"]
            data.append(item)
        return data,last_id
    except Exception as e :
        print(e)
    return [],last_id


async def insert_to_Db(data):
    async with get_db() as db:
        for x in data:
            x.update({"created_at" : datetime.now()})
            db.binance.insert_one(x)

last_id = 0

# 해당 dataFrame 을 분단위로 집계한다

def transform(data : pd.DataFrame):
    data = data.astype({"id":int,"price":float,"qty":float,"isBuyerMaker":"boolean","quoteQty":float})
    data["time"] = data.apply(lambda x: convert_unix_to_date(x["time"]),axis = 1)
    data["binance_taker_buy_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] == False else 0,axis = 1)
    data["binance_taker_sell_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] else 0,axis = 1)
    return data

def aggregate(data:pd.DataFrame):
    data["binance_taker_buy_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] == False else 0,axis = 1)
    data["binance_taker_sell_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] else 0,axis = 1)
    data = data.groupby("time").agg({"binance_taker_buy_vol":sum,"binance_taker_sell_vol":sum}).reset_index()
    return data.to_dict(orient = "records")

df = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
# 현재 시점(분단위까지) 기록한후
# 다음 분단위가 달라지는순간 start <= x < end_time 까지 데이터들을 종합한다.
# 현재 시점 보다 더 작은


# 시간 동일하게 맞춘후
start_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
logging.info(f" binance system is started at {start_date}")
while True:
    try :
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
        data,last_id = get_binance_data(last_id)
        # 계속 데이터를 추가하고
        if data :
            data = transform(pd.DataFrame(data))
            df = pd.concat([df,data],ignore_index = True)
        # 분 단위가 달라지는 순간
        if start_date != now:
            logging.info(f"{start_date}  -  {now} : preprocessing started")
            start_date = now
            # 데이터를 분리한다 .
            df = df.drop_duplicates()
            df = df.sort_values(by = "time").reset_index(drop = True)
            temp = df[(df["time"] < now)]
            if len(temp) > 0:
                index = temp.index[-1] + 1
                data,df = df.iloc[:index,:],df.iloc[index :,:]
                # 달라진 시점 기록하고
                db_data = aggregate(data)
                print(db_data)
                asyncio.run(insert_to_Db(db_data))
                logging.info(f"{start_date}  -  {now} : Loading into database completed successfully!!")
        # 자주호출하면 IP Ban 먹을수도있으므로 10초마다 호출
        time.sleep(10)
    except KeyError as k :
        print(k)
        time.sleep(60 * 10)
    except Exception as e :
        print(e)


