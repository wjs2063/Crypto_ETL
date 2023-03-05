from typing import List,Optional
from pydantic import BaseModel,Field,Required,validator
from enum import Enum
from datetime import datetime
from flask_wtf import FlaskForm
from wtforms import SelectField

class StockEnum(Enum):
    binance = "binance"
    bybit = "bybit"
    bitmex = "bitmex"
    all_exchange = "all_exchange"

class form(FlaskForm):
    stockMarket = SelectField("stockMarket",choices = [("binance","binance_exc"),("bybit","bybit_exc"),("bitmex","bitmex_exc"),("all_exchange","all")])

class RequestFormDataModel(BaseModel):
    stockMarket: StockEnum
    startDate : datetime
    endDate : datetime

    class Config:
        validate_assignment = True
        arbitrary_types_allowed = True
        validate_all = True
        extra = "forbid"

    @validator("*")
    def validate_func(cls,key):
        if not key:
            raise ValueError(f'{cls.__name__} field is required')
        return key

