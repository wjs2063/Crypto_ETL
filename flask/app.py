from flask import Flask,jsonify
from pydantic import BaseModel
from pymongo import MongoClient
from bson import json_util
from model.model_schema import StockEnum,RequestFormDataModel
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
from typing import List
from flask_pydantic import validate

app = Flask(__name__)



@app.route("/")
def index():
    return "hello World!"

@app.route("/taker_volume",methods = ["GET"])
def take_volumne(formData:RequestFormDataModel ):
    return "hello taker_volume"


