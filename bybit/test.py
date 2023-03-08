import requests
from datetime import datetime,timedelta
import pandas as pd
import time
from database import get_db
import asyncio
import logging
from typing import List,Optional
from constant import *
logging.basicConfig(filename = 'logs/info.log', encoding = 'utf-8', level = logging.INFO)



class BYBIT:
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
        data = data[data["time"] >= start_date]
        df = pd.concat([df,data],ignore_index = True)
        df = df.reset_index(drop = True)
        logging.info("concatenation function is finished!!")
        return df

    def seperate_data(self,df:pd.DataFrame,start_date,now) -> List[tuple]:
        """
        seperate_data function -> 현재까지받아온 데이터중에서 now 기준 x분.00초 <= t < (x + 1)분.00초 데이터로 분리해낸다.
        :param df: pd.DataFrame
        :return: (pd.DataFrame,pd.DataFrame)
        """
        logging.info("seperate_data function is started!!")
        temp = df[(df["time"] < now)]
        if len(temp) > 0 :
            index = temp.index[-1] + 1
            # data 분리
            data,df = df.iloc[:index,:],df.iloc[index:,:]
            data = self.aggregate(data)
            # data 만 처리
        else:
            data = [{"time":start_date,
                     BYBIT_TAKER_BUY_VOL:0,
                     BYBIT_TAKER_SELL_VOL:0
                     }]
        logging.info("separate function is started!!")
        return df,data



    def get_bybit_data(self) -> List[Optional[dict]]:
        param = {
            "symbol" : SYMBOL,
            "limit" : LIMIT
        }
        response = requests.get(BYBIT_URL ,params = param).json()
        if not response.get("result"):return []
        response = response["result"]
        # 시간순 정렬
        #response.sort(key = lambda x: x["time"])
        #시간변환
        return response

    def transform_time_format(self,df : pd.DataFrame) -> pd.DataFrame:
        data = df.astype({"id":int,"price":float,"qty":float,})
        df["time"] = df["time"].apply(lambda x: self.convert_iso_form(x))
        df["time"] = df["time"].apply(lambda x: self.convert_unix_to_date(x))
        return df

    def aggregate(self,data:pd.DataFrame) -> List[dict]:
        """
        aggregate function : time 을 기준으로 Taker_sell_vol,Taker_buy_vol 을 집계한다.
        :param data: pd.DataFrame
        :return: List[dict]
        """
        # avoid slice warning error
        data = data.copy()
        data[BYBIT_TAKER_BUY_VOL] = data.apply(lambda x: x["price"] * x["qty"] if x["side"] == "Buy" else 0,axis = 1)
        data[BYBIT_TAKER_SELL_VOL] = data.apply(lambda x: x["price"] * x["qty"] if x["side"] == "Sell" else 0,axis = 1)
        data = data.groupby("time").agg({BYBIT_TAKER_BUY_VOL:sum,BYBIT_TAKER_SELL_VOL:sum}).reset_index()
        return data.to_dict(orient = "records")

    def preprocessing(self,df,start_date,now):
        logging.info(f"{start_date}  -  {now} : preprocessing started")
        #중복제거
        df = df.drop_duplicates()
        # 시간기준 오름차순 정렬
        df = df.sort_values(by = "time").reset_index(drop = True)
        # now 기준으로 data 분리
        after,before = self.seperate_data(df,start_date,now)
        return after,before

    def excute(self):
        start_date = self.get_current_time()
        df = pd.DataFrame(columns = ["id","symbol","price","qty","side","time"])
        while True:
            try :
                now = datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')
                bybit_data = self.get_bybit_data()
                if bybit_data:
                    df = self.concatenate(df,start_date,bybit_data)
                #데이터 이어붙히고
                if start_date != now:
                    logging.info(f"{start_date}  -  {now} : preprocessing started")
                    df,db_data = self.preprocessing(df,start_date,now)
                    #DB 적재
                    print(db_data)
                    asyncio.get_event_loop().run_until_complete(self.insert_to_database(db_data))
                    logging.info(f"{start_date}  -  {now} : Loading into database completed successfully!!")
                    start_date = now
                time.sleep(20)
            except KeyError as k:
                logging.error(f"Key Error:{k}")
                time.sleep(60 * 10)
            except Exception as e:
                logging.error(f"Exception Error: {e}")
                time.sleep(60)
    async def insert_to_database(self,data : List[Optional[dict]]):
        async with get_db() as db:
            for doc in data:
                doc.update({"created_at" : datetime.now()})
                db.bybit.insert_one(doc)

Bybit = BYBIT()
Bybit.excute()