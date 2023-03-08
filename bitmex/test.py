import requests
from datetime import datetime,timedelta
import pandas as pd
import time
from database import get_db
import asyncio
import logging
from typing import List,Optional
from constant import *
"""
time stamp : utc iso format
side : Buy,Sell
size : 수량
price : 가격
"""


logging.basicConfig(filename = 'logs/info.log', encoding = 'utf-8', level = logging.INFO)

# {'error': {'message': 'Rate limit exceeded, retry in 10 minutes.', 'name': 'RateLimitError'}}

class BITMEX:
    def __init__(self):
        pass

    def get_current_time(self):
        return datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')

    def convert_unix_to_date(self,time):
    # 년 월 일 시 분 까지만 가져온다 (분단위)
        time /= 1000
        return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

    def convert_iso_form(self,time):
        timestamp = int(datetime.fromisoformat(time[:-1]).timestamp() * 1000 + int(time[-4:-1]))
        return timestamp


    def get_bitmex_data(self,startTime:datetime,endTime:datetime) -> List[Optional[dict]]:
        """
        get_bitmex_data function : bitmext_url 에서 최근으로부터 limit 만큼의 데이터를 가져와 json 형식으로 변환한다.
        :param startTime:
        :param endTime:
        :return: List[Optional[dict]]
        """
        logging.info("get_bitmex_data function is started!!")
        bitmex_url = BITMEX_URL
        param = {
            'symbol': SYMBOL,
            'count': COUNT,
            "startTime":startTime,
            "endTime:":endTime
        }
        response = requests.get(BITMEX_URL ,params = param).json()
        logging.info("get_bitmex_data function is finished!!")
        return response

    def transform_time_format(self,df:pd.DataFrame) -> pd.DataFrame:
        """
        transform function : dataFrame 에 데이터의 형변환 그리고 시간 타입을 datetime 으로 변환한다.
        :param df: pd.DataFrame
        :return: pd.DataFrame
        """
        logging.info("transform function is started!!")
        df = df.astype({"trdMatchID":str,"price":float,"size":float,})
        df["timestamp"] = df["timestamp"].apply(lambda x: self.convert_iso_form(x))
        df["timestamp"] = df["timestamp"].apply(lambda x: self.convert_unix_to_date(x))
        logging.info("transform function is finished!!")
        return df

    def aggregate(self,data:pd.DataFrame) -> List[dict]:
        """
        aggregate function : time 을 기준으로 Taker_sell_vol,Taker_buy_vol 을 집계한다.
        :param data: pd.DataFrame
        :return: List[dict]
        """
        # avoid slice warning error
        data = data.copy()
        logging.info("aggregate function is started!!")
        data[BITMEX_TAKER_BUY_VOL] = data.apply(lambda x: x["price"] * x["size"] if x["side"] == "Buy" else 0,axis = 1)
        data[BITMEX_TAKER_SELL_VOL] = data.apply(lambda x: x["price"] * x["size"] if x["side"] == "Sell" else 0,axis = 1)
        data = data.groupby("time").agg({BITMEX_TAKER_BUY_VOL:sum,BITMEX_TAKER_SELL_VOL:sum}).reset_index()
        logging.info("aggrgate function is finished!!")
        return data.to_dict(orient = "records")


    def concatenate(self,df:pd.DataFrame,start_date,data:pd.DataFrame):
        """
        concatenation function : 두개의 data 를 이어붙힌다.
        :param df: pd.DataFrame
        :param data: pd.DataFrame
        :return:
        """
        logging.info("concatenation function is started!!")
        data = pd.DataFrame(data)
        data = self.transform_time_format(data)
        data = data[data["timestamp"] >= start_date]
        df = pd.concat([df,data],ignore_index = True)
        df = df.reset_index(drop = True)
        logging.info("concatenation function is finished!!")
        return df


    def seperate_data(self,df:pd.DataFrame,start_date,now) -> List[tuple]:
        """
        seperate_data function -> 현재까지받아온 데이터중에서 x분.00초 <= t < (x + 1)분.00초 데이터로 분리해낸다.
        :param df: pd.DataFrame
        :return: (pd.DataFrame,pd.DataFrame)
        """
        logging.info("seperate_data function is started!!")
        temp = df[(df["timestamp"] < now)]
        if len(temp) > 0 :
            index = temp.index[-1] + 1
            # data 분리
            data,df = df.iloc[:index,:],df.iloc[index:,:]
            data = data.rename(columns = {"timestamp":"time"})
            data = self.aggregate(data)
            # data 만 처리
        else:
            data = [{"time":start_date,
                     BITMEX_TAKER_BUY_VOL:0,
                     BITMEX_TAKER_SELL_VOL:0
                     }]
        logging.info("separate function is started!!")
        return df,data

    async def insert_to_database(self,data : List[dict]):
        async with get_db() as db:
            for doc in data:
                doc.update({"created_at" : datetime.now()})
                db.bitmex.insert_one(doc)


    def preprocessing(self,df,data,start_date,now):
        if data:
            df = self.concatenate(df,start_date,data)
            # 중복 제거
        df = df.drop_duplicates()
        # 시간순으로 ASC 정렬
        df = df.sort_values(by = "timestamp").reset_index(drop = True)
        # now 기준으로 데이터 분리
        after,before = self.seperate_data(df,start_date,now)
        return after,before
    def excute(self):
        start_date = self.get_current_time()
        df = pd.DataFrame(columns = ['timestamp', 'symbol', 'side', 'size', 'price', 'tickDirection', 'trdMatchID', 'grossValue', 'homeNotional', 'foreignNotional', 'trdType'])

        while True:
            try :
                now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
                if start_date != now:
                    logging.info(f"{start_date}  -  {now} : preprocessing started")

                    bitmex_data = self.get_bitmex_data(start_date,now)
                    df,db_data = self.preprocessing(df,bitmex_data,start_date,now)
                    # Async -> Load to Database
                    print(db_data)
                    asyncio.get_event_loop().run_until_complete(self.insert_to_database(db_data))
                    logging.info(f"{start_date}  -  {now} : Loading into database completed successfully!!")
                    # 바뀐시간 기록
                    start_date = now
                time.sleep(20)
            except KeyError as k:
                logging.error(f"Key Error:{k}")
                print(k)
                time.sleep(60 * 10)
            except Exception as e:
                logging.error(f"Exception Error: {e}")
                print(e)
                time.sleep(60)


Bitmex = BITMEX()
Bitmex.excute()


