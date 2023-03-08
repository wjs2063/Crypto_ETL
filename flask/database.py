from pymongo import MongoClient

import contextlib
import logging
from config.constant import *
logging.basicConfig(filename = './logs/database.log', encoding = 'utf-8', level = logging.INFO)
from config.constant import *



@contextlib.contextmanager
def sync_db():
    db = MongoClient(MONGO_DETAILES)
    try:
        yield db.local
        logging.info("Mongo database connected!! ")
    finally:
        db.close()
        logging.info("Mongo database disconnected!! ")

def get_exchangedata_from_database(exchange_name,startTime,endTime,limit):
    with sync_db() as db:
        data = list(db[f"{exchange_name}"].find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,f"{exchange_name}_taker_sell_vol":1,f"{exchange_name}_taker_buy_vol":1,"_id":0}).sort("time",-1).limit(limit))
        return data
def get_all_exchange_data(startTime,endTime,limit):
    logging.info("get_all_exchange_data started!!")
    binance_data = get_exchangedata_from_database(BINANCE,startTime,endTime,limit)
    bybit_data = get_exchangedata_from_database(BYBIT,startTime,endTime,limit)
    bitmex_data = get_exchangedata_from_database(BITMEX,startTime,endTime,limit)
    answer = []

    for binance,bybit,bitmex in zip(binance_data,bybit_data,bitmex_data):
        temp_data = dict()
        temp_data["time"] = binance["time"]
        temp_data[ALL_EXCHANGE_TAKER_BUY_VOL] = binance[BINANCE_TAKER_BUY_VOL] + bybit[BYBIT_TAKER_BUY_VOL] + bitmex[BITMEX_TAKER_BUY_VOL]
        temp_data[ALL_EXCHANGE_TAKER_SELL_VOL] = binance[BINANCE_TAKER_SELL_VOL] + bybit[BYBIT_TAKER_SELL_VOL] + bitmex[BITMEX_TAKER_SELL_VOL]
        answer.append(temp_data)

    logging.info("get_all_exchange_data finished!!")
    return answer



def get_exchange_data(exchange_name,startTime,endTime,limit):
    logging.info("get_exchange_data started!!")
    if exchange_name == BINANCE:
        return get_exchangedata_from_database(BINANCE,startTime,endTime,limit)
    elif exchange_name == BYBIT:
        return get_exchangedata_from_database(BYBIT,startTime,endTime,limit)
    elif exchange_name == BITMEX:
        return get_exchangedata_from_database(BITMEX,startTime,endTime,limit)
    else:
        return get_all_exchange_data(startTime,endTime,limit)
    logging.info("get_exchange_data finished!!")

