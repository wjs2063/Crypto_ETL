from pymongo import MongoClient

import contextlib
import logging
from config.CONST import *
logging.basicConfig(filename = './logs/database.log', encoding = 'utf-8', level = logging.INFO)
MONGO_DETAILS = "mongodb://172.30.1.56:45000/test"


# make user_database

#db = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
@contextlib.asynccontextmanager
async def get_db():
    db = MongoClient(MONGO_DETAILS)
    try:
        yield db.local
    except Exception as e:
        print(e)
    finally:
        db.close()

@contextlib.contextmanager
def sync_db():
    db = MongoClient(MONGO_DETAILS)
    try:
        yield db.local
        logging.info("Mongo database connected!! ")
    finally:
        db.close()
        logging.info("Mongo database disconnected!! ")

def get_exchangedata_from_database(exchange_name,startTime,endTime):
    with sync_db() as db:
        data = list(db[f"{exchange_name}"].find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,f"{exchange_name}_taker_sell_vol":1,f"{exchange_name}_taker_buy_vol":1,"_id":0}).sort("time",-1).limit(100))
        return data
def get_all_exchange_data(startTime,endTime):
    logging.info("get_all_exchange_data started!!")
    binance_data = get_exchangedata_from_database("binance",startTime,endTime)
    bybit_data = get_exchangedata_from_database("bybit",startTime,endTime)
    bitmex_data = get_exchangedata_from_database("bitmex",startTime,endTime)
    answer = []

    for binance,bybit,bitmex in zip(binance_data,bybit_data,bitmex_data):
        temp_data = dict()
        temp_data["time"] = binance["time"]
        temp_data["all_exchange_taker_buy_vol"] = binance[BINANCE_TAKER_BUY_VOL] + bybit[BYBIT_TAKER_BUY_VOL] + bitmex[BITMEX_TAKER_BUY_VOL]
        temp_data["all_exchange_taker_sell_vol"] = binance[BINANCE_TAKER_SELL_VOL] + bybit[BYBIT_TAKER_SELL_VOL] + bitmex[BITMEX_TAKER_SELL_VOL]
        answer.append(temp_data)

    logging.info("get_all_exchange_data finished!!")
    return answer



def get_exchange_data(exchange_name,startTime,endTime):
    logging.info("get_exchange_data started!!")
    if exchange_name == "binance":
        return get_exchangedata_from_database("binance",startTime,endTime)
    elif exchange_name == "bybit":
        return get_exchangedata_from_database("bybit",startTime,endTime)
    elif exchange_name == "bitmex":
        return get_exchangedata_from_database("bitmex",startTime,endTime)
    else:
        return get_all_exchange_data(startTime,endTime)
    logging.info("get_exchange_data finished!!")

