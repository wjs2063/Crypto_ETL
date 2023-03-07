
import motor.motor_asyncio
import contextlib
import logging


logging.basicConfig(filename = 'logs/database.log', encoding = 'utf-8', level = logging.INFO)
MONGO_DETAILS = "mongodb://172.30.1.56:45000"


@contextlib.asynccontextmanager
async def get_db():
    db = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
    try:
        yield db.local
        logging.info("Mongo database connected!! ")
    finally:
        db.close()
        logging.info("Mongo database disconnected!! ")


#db = MongoClient(MONGO_DETAILS)
#print(db.local.data.find({"time":"2023-03-03 14:05"}))