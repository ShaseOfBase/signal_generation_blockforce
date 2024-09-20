from abc import abstractmethod
import logging
import traceback
import typing
import uuid
from collections import defaultdict

import requests
import slack_sdk
from config import SLACK_TOKEN
from pydantic import BaseModel
from sqlalchemy.orm import Session
from lib.data_client import DataBaseClient, DataClient
from lib.dev_mode import get_dev_mode
from lib.models import StrategyConfig, SystemConfig

# from trading_app_helpers.crud import crud_get_alloc


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
    current_regime_class: typing.Optional[int] = None
    sl_stop: typing.Optional[float] = None
    tp_stop: typing.Optional[float] = None


class StrategyClient(DataBaseClient):
    first_update = True

    def __init__(
        self,
        strategy_name: str,
        data_client: DataClient,
        symbol: str,
        system_configs: typing.List[SystemConfig],
        strategy_config: StrategyConfig,
        logger: logging.Logger,
    ):
        super().__init__(system_config=system_configs[0])
        self.slack_client = slack_sdk.WebClient(token=SLACK_TOKEN)
        self.strategy_name = strategy_name
        self.data_client = data_client
        self.symbol = symbol
        self.system_configs = system_configs
        self.strategy_config = strategy_config
        self.df_1h = None
        self.stale_data = False
        self.logger = logger
        self.strategy_params = strategy_config.strategy

    def send_signal(
        self,
        system: SystemConfig,
        strategy_name: str,
        trade_type: str,
        perc_equity: float = None,
        current_regime_class: int = None,
        sl_stop: float = None,
        tp_stop: float = None,
    ):
        """
        trade_type will be Entry Long, Entry Short, Exit Position
        """
        self.logger.info(f"Sending signal to {system.name}")

        if get_dev_mode():
            self.logger.info("Signal generated, dev mode is on, logging only...")
            self.logger.info(f"System: {system}")
            self.logger.info(f"Strategy: {strategy_name}")
            self.logger.info(f"Trade Type: {trade_type}")
            self.logger.info(f"Perc Equity: {perc_equity}")
            return

        url = system.trading_url
        try:
            signal = YosemiteSignalSchema(
                strategy=strategy_name,
                type=trade_type,
                slippage=0.1,
                interval=str(uuid.uuid4()),  # not sure if this is needed
                perc_equity=perc_equity,
                ignore_two_min_interval=True,
                current_regime_class=current_regime_class,
                sl_stop=sl_stop,
                tp_stop=tp_stop,
            )

            signal_dict = signal.model_dump(exclude_none=True)
            response = requests.post(url, json=signal_dict)
            return response
        except:
            self.logger.error(traceback.format_exc())

    def send_message(
        self,
        strategy: str,
        msg: str,
        channel: str | None,
    ):
        """
        :innocent_cat:
        :grimacing_cat:
        """

        if get_dev_mode():
            self.logger.info(f"Dev mode enabled, not sending to slack")
            self.logger.info(msg)
            return

        if not channel:
            self.logger.info("No channel specified, logging msg only...")
            self.logger.info(msg)

        internal_channel = channel
        msg = strategy + ": " + msg
        try:
            res = self.slack_client.conversations_list(types="public_channel")
            channels = res["channels"]
            dd = [x for x in channels if x["name"] == internal_channel]
            if len(dd) == 0:
                self.logger.error(f"Channel {internal_channel} does not exist.")
                return

            channel_id = dd[0]["id"]
            res = self.slack_client.conversations_join(channel=channel_id)
        except Exception as e:
            self.logger.error(traceback.format_exc())
        try:
            self.slack_client.chat_postMessage(channel=internal_channel, text=msg)
        except Exception as e:
            self.logger.error(traceback.format_exc())

    @abstractmethod
    def generate_signal(self):
        raise NotImplementedError
