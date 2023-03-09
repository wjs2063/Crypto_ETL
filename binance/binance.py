import requests
from datetime import datetime
import pandas as pd
from database import get_db
import time
import asyncio
from typing import List,Optional,Tuple
from constant import *
import logging

logging.basicConfig(filename='./logs/info.log', encoding = 'utf-8', level = logging.INFO)


class Binance:

    def __init__(self):
        pass

    def get_current_time(self) -> datetime :
        return datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M')

    def convert_unix_to_date(self, time) -> datetime:
        # 년 월 일 시 분 까지만 가져온다 (분단위)
        time /= 1000
        return datetime.fromtimestamp(time).strftime('%Y-%m-%d %H:%M')

    def convert_iso_form(self, time)  :
        timestamp = int(datetime.fromisoformat(time[:-1]).timestamp() * 1000 + int(time[-4:-1]))
        return timestamp

    def get_binance_data(self) -> List[Optional[dict]]:
        try:
            binance_param = {
                "symbol": SYMBOL,
                "limit": LIMIT
            }
            response = requests.get(BINANCE_URL, params = binance_param).json()

            return response
        except Exception as e:
            print(e)
        return response

    def transform_time_format(self,df : pd.DataFrame) -> pd.DataFrame:
        df = df.astype({"id":int,"price":float,"quoteQty":float,"qty":float,"isBuyerMaker":"boolean"})
        df["time"] = df["time"].apply(lambda x: self.convert_unix_to_date(x))
        return df

    def seperate_data(self, df: pd.DataFrame,start_date,now) -> Tuple[pd.DataFrame,pd.DataFrame] :
        """
        separate_data function -> 현재까지받아온 데이터중에서 x분.00초 <= t < (x + 1)분.00초 데이터로 분리해낸다.
        :param df: pd.DataFrame
        :return: (pd.DataFrame,pd.DataFrame)
        """
        logging.info("separate_data function is started!!")
        temp = df[(df["time"] < now)]
        if len(temp) > 0:
            index = temp.index[-1] + 1
            # data 분리
            data, df = df.iloc[:index, :], df.iloc[index:, :]
            data = self.aggregate(data)
            # data 만 처리
        else:
            data = [{"time": start_date,
                     BINANCE_TAKER_BUY_VOL: 0,
                     BINANCE_TAKER_SELL_VOL: 0
                     }]
        logging.info("separate function is started!!")
        return df, data

    def aggregate(self,data:pd.DataFrame) -> List[dict]:
        """
        aggregate function : time 을 기준으로 Taker_sell_vol,Taker_buy_vol 을 집계한다.
        :param data: pd.DataFrame
        :return: List[dict]
        """
        data = data.copy()
        data[BINANCE_TAKER_BUY_VOL] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] == False else 0,axis = 1)
        data[BINANCE_TAKER_SELL_VOL] = data.apply(lambda x: x["quoteQty"] if x["isBuyerMaker"] else 0,axis = 1)
        data = data.groupby("time").agg({BINANCE_TAKER_BUY_VOL:sum,BINANCE_TAKER_SELL_VOL:sum}).reset_index()
        return data.to_dict(orient = "records")


    def concatenate(self, df: pd.DataFrame,start_date:datetime , data: pd.DataFrame,) -> pd.DataFrame:
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
        df = pd.concat([df, data], ignore_index = True)
        df = df.reset_index(drop = True)
        logging.info("concatenation function is finished!!")
        return df

    def preprocessing(self,df,start_date,now) -> Tuple[pd.DataFrame,pd.DataFrame]:
        #중복제거
        df = df.drop_duplicates()
        # 시간기준 오름차순 정렬
        df = df.sort_values(by = "time").reset_index(drop = True)
        # now 기준으로 data 분리
        after,before = self.seperate_data(df,start_date,now)
        return after,before


    def excute(self) -> None:
        start_date = self.get_current_time()
        df = pd.DataFrame(columns = ['id', 'price', 'qty', 'quoteQty', 'time', 'isBuyerMaker'])
        while True:
            try :
                now = self.get_current_time()
                binance_data = self.get_binance_data()
                # 계속 데이터를 추가하고
                if binance_data :
                    df = self.concatenate(df,start_date,binance_data,)
                # 분 단위가 달라지는 순간
                if start_date != now:
                    df,db_data = self.preprocessing(df,start_date,now)
                    asyncio.get_event_loop().run_until_complete(self.insert_to_database(db_data))
                    start_date = now
                # 자주호출하면 IP Ban 먹을수도있으므로 10초마다 호출
                time.sleep(10)
            except KeyError as k:
                logging.error(f"Key Error:{k}")
                time.sleep(60 * 10)
            except Exception as e:
                logging.error(f"Exception Error: {e}")
                time.sleep(60)
    async def insert_to_database(self, data: List[Optional[dict]]) -> None:
        async with get_db() as db:
            for doc in data:
                doc.update({"created_at": datetime.now()})
                db.binance.insert_one(doc)
        logging.info("Loading into database completed successfully!!")


Binance = Binance()
Binance.excute()



