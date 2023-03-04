from pymongo import MongoClient
import contextlib

MONGO_DETAILS = "mongodb://172.30.1.56:45000/test"


# make user_database

#db = motor.motor_asyncio.AsyncIOMotorClient(MONGO_DETAILS)
@contextlib.asynccontextmanager
async def get_db():
    db = MongoClient(MONGO_DETAILS)
    try:
        yield db.local
    finally:
        db.close()


db = MongoClient(MONGO_DETAILS)
print(db.local.data.find({"time":"2023-03-03 14:05"}))