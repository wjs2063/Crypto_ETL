from flask import Flask, abort
import json
from enum import Enum
from database import get_exchange_data
from flask_restx import Api, Resource
from datetime import datetime
from bson import json_util
import logging

logging.basicConfig(filename = 'info.log', encoding = 'utf-8', level = logging.DEBUG)
app = Flask(__name__)

api = Api(app)

class Market_Enum(Enum):
    binance = "binance"
    bybit = "bybit"
    bitmex = "bitmex"
    all_exchange = "all_exchange"


# Example 1: query parameters only
@api.route("/test/<string:stockMarket><string:startTime><string:endTime>")
class Index(Resource):
    async def get(self,stockMarket,startTime,endTime):
        if stockMarket not in ["binance","bybit","bitmex","all_exchange"]:
            abort(404,"stockMarket can only have 4 types. binance,bybit,bitmex,all_exchange")
        return {"stockMarket":stockMarket,"startTime":startTime,"endTime":endTime}



@api.route("/taker_volume/<string:stockmarket>/<string:starttime>/<string:endtime>")
class Index(Resource):
    def get(self, stockmarket,starttime,endtime):
        if stockmarket not in ["binance", "bybit", "bitmex","all_exchange"]:
            abort(404, "stockMarket can only have 4 types. binance,bybit,bitmex,all_exchange")
        try:
            start_dt = datetime.strptime(starttime, "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(endtime, "%Y-%m-%d %H:%M")
        except ValueError:
            abort(400, "Invalid format. 데이터형식은 'YYYY-MM-DD HH:MM' 으로 작성하세요.")
        except Exception as e :
            abort(500,"Inter Server Error Retry!")
        data = get_exchange_data(stockmarket,starttime,endtime)
        #with sync_db() as db:
            #data = list(db.binance.find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,"binance_taker_sell_vol":1,"binance_taker_buy_vol":1,"_id":0}))
        #async with get_db() as db:
            #await db["binance"].find({"time":{'$gte':startTime,'$lt':endTime}})
        #data = [d async for d in data]
        return {"stockmarket": stockmarket,"starttime":starttime,"endtime":endtime,"data":json.loads(json_util.dumps(data))
                }


