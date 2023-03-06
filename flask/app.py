from flask import Flask, abort
import json
from enum import Enum
from config.database import sync_db,get_exchange_data
from flask_restx import Api, Resource
from datetime import datetime
from bson import json_util
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





@api.route("/gg/<string:stockMarket>/<string:startTime>/<string:endTime>")
class Index(Resource):
    def get(self, stockMarket,startTime,endTime):
        if stockMarket not in ["binance", "bybit", "bitmex","all_exchange"]:
            abort(404, "stockMarket can only have 4 types. binance,bybit,bitmex,all_exchange")
        try:
            start_dt = datetime.strptime(startTime, "%Y-%m-%d %H:%M")
            end_dt = datetime.strptime(endTime, "%Y-%m-%d %H:%M")
        except ValueError:
            abort(400, "Invalid format. 데이터형식은 'YYYY-MM-DD HH:MM' 으로 작성하세요.")
        data = get_exchange_data(stockMarket,startTime,endTime)
        #with sync_db() as db:
            #data = list(db.binance.find({"time":{'$gte':startTime,'$lte':endTime}},{"time":1,"binance_taker_sell_vol":1,"binance_taker_buy_vol":1,"_id":0}))
        #async with get_db() as db:
            #await db["binance"].find({"time":{'$gte':startTime,'$lt':endTime}})
        #data = [d async for d in data]
        return {"stockMarket": stockMarket,"startTime":startTime,"endTime":endTime,"data":json.loads(json_util.dumps(data))
                }


