from flask import Flask,jsonify
from pymongo import MongoClient
from bson import json_util
import asyncio
import json
from flask_request_validator import (
    PATH,
    FORM,
    Param,
    Pattern,
    validate_params
)
from enum import Enum
from database import get_db
app = Flask(__name__)



@app.route("/")
def index():
    return "hello World!"

@app.route("/taker_volume",methods = ["GET"])
def take_volumne():
    MONGO_DETAILS = "mongodb://172.30.1.56:45000/test"
    db = MongoClient(MONGO_DETAILS)
    data = db.local.data.find()
    return json_util.dumps(data)

