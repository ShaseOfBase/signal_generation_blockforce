from pydantic import BaseModel
import typing
from dataclasses import dataclass



# This Schema is duplicated in pm_ee/pm_app/schemas/yosemite.py
class YosemiteSignalSchema(BaseModel):
    passphrase: typing.Optional[str] = None
    strategy: str
    ticker: typing.Optional[str] = None
    exchange: typing.Optional[str] = None
    close: typing.Optional[str] = None
    time: typing.Optional[str] = None
    interval: typing.Optional[str]
    type: str
    perc_equity: typing.Optional[float] = None
    depth_threshold_ratio: typing.Optional[float] = None
    depth_amount: typing.Optional[int] = None
    slippage: typing.Optional[float]
    text: typing.Optional[str] = None

    force: typing.Optional[bool] = False
    ignore_two_min_interval: typing.Optional[bool] = False

@dataclass
class System:
    name:str 
    db_url:str = None
    trading_url:str = None
    accounts:typing.List[str] = None
