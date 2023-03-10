#-*- coding:utf-8 -*-
import requests
from datetime import datetime
import pandas as pd
from database import get_db
import time
import asyncio
from typing import List,Optional
import logging
logging.basicConfig(filename = './logs/info.log', encoding = 'utf-8', level = logging.DEBUG)
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


def convert_unix_to_date(time):
    # 년 월 일 시 분 까지만 가져온다 (분단위)
    time /= 1000
    return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

def convert_iso_form(time):
    timestamp = int(datetime.fromisoformat(time[:-1]).timestamp() * 1000 + int(time[-4:-1]))
    return timestamp
def get_binance_data() -> List[Optional[dict]]:
    try :
        binance_url = "https://fapi.binance.com/fapi/v1/trades"
        data = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
        binance_param = {
            "symbol": "BTCUSDT",
            "limit":1000
        }
        response = requests.get(binance_url,params = binance_param).json()
        # 가져온 데이터들중 last_id 보다 큰것만 계속 넣는다

        return response
    except Exception as e :
        print(e)
    return response

def seperate_data(df:pd.DataFrame,now) -> List[tuple]:
    """
    seperate_data function -> 현재까지받아온 데이터중에서 x분.00초 <= t < (x + 1)분.00초 데이터로 분리해낸다.
    :param df: pd.DataFrame
    :return: (pd.DataFrame,pd.DataFrame)
    """
    logging.info("seperate_data function is started!!")
    temp = df[(df["time"] < now)]
    if len(temp) > 0 :
        index = temp.index[-1] + 1
        # data 분리
        data,df = df.iloc[:index,:],df.iloc[index:,:]
        data = aggregate(data)
        # data 만 처리
    else:
        data = [{"time":start_date,
                 "binance_taker_buy_vol":0,
                 "binance_taker_sell_vol":0
                 }]
    logging.info("separate function is started!!")
    return df,data

def concatenate(df:pd.DataFrame,data:pd.DataFrame):
    """
    concatenation function : 두개의 data 를 이어붙힌다.
    :param df: pd.DataFrame
    :param data: pd.DataFrame
    :return:
    """
    logging.info("concatenation function is started!!")
    data = pd.DataFrame(data)
    data = transform_time_format(data)
    data = data[data["time"] >= start_date]
    df = pd.concat([df,data],ignore_index = True)
    df = df.reset_index(drop = True)
    logging.info("concatenation function is finished!!")
    return df

async def insert_to_database(data : List[Optional[dict]]):
    async with get_db() as db:
        for doc in data:
            doc.update({"created_at" : datetime.now()})
            db.binance.insert_one(doc)


def transform_time_format(df : pd.DataFrame) -> pd.DataFrame:
    df = df.astype({"id":int,"price":float,"quoteQty":float,"qty":float,"isBuyerMaker":"boolean"})
    df["time"] = df["time"].apply(lambda x: convert_unix_to_date(x))
    return df

def aggregate(data:pd.DataFrame) -> List[dict]:
    """
    aggregate function : time 을 기준으로 Taker_sell_vol,Taker_buy_vol 을 집계한다.
    :param data: pd.DataFrame
    :return: List[dict]
    """
    data = data.copy()
    data["binance_taker_buy_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] == False else 0,axis = 1)
    data["binance_taker_sell_vol"] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] else 0,axis = 1)
    data = data.groupby("time").agg({"binance_taker_buy_vol":sum,"binance_taker_sell_vol":sum}).reset_index()
    return data.to_dict(orient = "records")

def preprocessing(df,now):
    logging.info(f"{start_date}  -  {now} : preprocessing started")
    #중복제거
    df = df.drop_duplicates()
    # 시간기준 오름차순 정렬
    df = df.sort_values(by = "time").reset_index(drop = True)
    # now 기준으로 data 분리
    after,before = seperate_data(df,now)
    return after,before





start_date = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
df = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
logging.info(f" binance system is started at {start_date}")
while True:
    try :
        now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
        binance_data = get_binance_data()
        # 계속 데이터를 추가하고
        if binance_data :
            df = concatenate(df,binance_data)
        # 분 단위가 달라지는 순간
        if start_date != now:
            df,db_data = preprocessing(df,now)
            #asyncio.run(insert_to_database(db_data))
            print(db_data)
            asyncio.get_event_loop().run_until_complete(insert_to_database(db_data))
            logging.info(f"{start_date}  -  {now} : Loading into database completed successfully!!")
            start_date = now
        # 자주호출하면 IP Ban 먹을수도있으므로 10초마다 호출
        time.sleep(10)
    except KeyError as k:
        logging.error(f"Key Error:{k}")
        print(k)
        time.sleep(60 * 10)
    except Exception as e:
        logging.error(f"Exception Error: {e}")
        print(e)
        time.sleep(60)

