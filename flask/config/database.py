from pymongo import MongoClient
import asyncio
import motor.motor_asyncio
import contextlib
import logging
logging.basicConfig(filename = 'logs/database.log', encoding = 'utf-8', level = logging.INFO)
MONGO_DETAILS = "mongodb://172.30.1.56:45000/test"


# make user_database

#db = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
@contextlib.asynccontextmanager
async def get_db():
    db = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS).local
    try:
        yield db
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

def sumOfvolumes(exchange_name,data):
    sell_vol,buy_vol = 0,0

    for v in data:
        sell_vol += v[f"{exchange_name}_taker_sell_vol"]
        buy_vol += v[f"{exchange_name}_taker_sell_vol"]
    return sell_vol,buy_vol


def get_exchange_data(exchange_name,startTime,endTime):
    with sync_db() as db:
        if exchange_name == "binance":
            data = list(db.binance.find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,f"{exchange_name}_taker_sell_vol":1,f"{exchange_name}_taker_buy_vol":1,"_id":0}).limit(100))
            return data
        elif exchange_name == "bybit":
            data = list(db.bybit.find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,f"{exchange_name}_taker_sell_vol":1,f"{exchange_name}_taker_buy_vol":1,"_id":0}).limit(100))
            return data
        elif exchange_name == "bitmex":
            data = list(db.bitmex.find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,f"{exchange_name}_taker_sell_vol":1,f"{exchange_name}_taker_buy_vol":1,"_id":0}).limit(100))
            return data
        else:
            return []

