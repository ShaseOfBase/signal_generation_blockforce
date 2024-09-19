import logging
from pathlib import Path
import tomllib
import vectorbtpro as vbt
from typing import List

import numpy as np

from config import SLACK_CHANNEL
from lib.data_client import DataClient
from lib.models import System
from lib.path_manager import PathManager
from lib.strategy_client import StrategyClient
from strategy_clients.psar_client.psar_signals import get_psar_signals_df


logger = logging.getLogger(__name__)


class SignalGeneratorPsar(StrategyClient):
    """
    Signal generator for the Parabolic SAR strategy
    """

    config_name = "psar_config.toml"

    def __init__(
        self,
        systems: List[System],
        strategy_name: str,
        data_client: DataClient,
        symbol: str,
    ):
        super().__init__(systems=systems)
        self.strategy_name = strategy_name
        self.data_client = data_client
        self.symbol = symbol
        self.accounts = systems
        self.df_1m = None
        self.stale_data = False

        self.initialize_config()
        self.initialize_data()

    def initialize_config(self):
        strategy_configs_path = PathManager.strategy_configs_path

        with open(strategy_configs_path / self.config_name, "rb") as f:
            psar_config = tomllib.load(f)

        self.strategy_params = psar_config.get("strategy", {})

    def initialize_data(self, retry_count=0):
        # We need 30 days of warmup data
        self.df_1m = self.data_client.get_historical_data(
            symbol=self.symbol,
            candle_length_minutes=1,
            number_of_candles=43_202,  # 43_200 needed as base to establish all values, +2 makes sure we're in signal gen territory
        )

        data = [self.df_1m]

        max_retries = 5
        if any(item is None for item in data) and retry_count < max_retries:
            msg = "Historical data fetching failed - retrying..."
            logger.info(msg)
            self.initialize_data(retry_count + 1)
        elif any(item is None for item in data):
            msg = (
                f"Historical data fetching failed for {self.strategy_name} "
                f"after {max_retries} retries - Exiting App"
            )
            logger.error(msg)
            self.send_message(self.strategy_name, msg, SLACK_CHANNEL)
            exit()

    def update_data(self) -> bool:
        self.df_1m, stale_1m, updated_1m = self.data_client.update_candles(
            self.symbol, self.df_1m, 1, 5
        )

        go = False

        if stale_1m:
            self.stale_data = True
            msg = f"Stale Data in Update Data | 1m Bar {stale_1m}"
            self.send_message(self.strategy_name, msg, SLACK_CHANNEL)
            logger.error(msg)
        else:
            self.stale_data = False

        if updated_1m:
            go = True

        return go

    def generate_signal(self):
        go = self.update_data()

        if not go:
            return

        if self.symbol in self.strategy_params["psar_settings"]:
            psar_settings = self.strategy_params["psar_settings"][self.symbol]
        else:
            psar_settings = self.strategy_params["psar_settings"]["default"]

        global_warmup_symbol_ohlcv_df = self.df_1m

        signals_df = get_psar_signals_df(
            global_warmup_symbol_ohlcv_df,
            trial_instance_params=self.strategy_params,
            psar_settings=psar_settings,
        )

        trade_sizes_lg_high_vol = np.array(
            self.strategy_params["trade_sizes_long"]["high_vol"],
            dtype=np.float64,
        )
        trade_sizes_lg_low_vol = np.array(
            self.strategy_params["trade_sizes_long"]["low_vol"],
            dtype=np.float64,
        )
        trade_sizes_st_high_vol = np.array(
            self.strategy_params["trade_sizes_short"]["high_vol"],
            dtype=np.float64,
        )
        trade_sizes_st_low_vol = np.array(
            self.strategy_params["trade_sizes_short"]["low_vol"],
            dtype=np.float64,
        )

        trade_sizes_long = np.array(
            [trade_sizes_lg_high_vol, trade_sizes_lg_low_vol], dtype=np.float64
        )

        trade_sizes_short = np.array(
            [trade_sizes_st_high_vol, trade_sizes_st_low_vol], dtype=np.float64
        )

        trade_conditions = np.array(
            [0 if x == "high_vol" else 1 for x in signals_df.trade_condition],
            dtype=np.int64,
        )
        # endregion

        if signals_df.final_signal.iloc[-1] in [1, -1]:

            logger.info(f"{self.strategy_name} signal generated")

            if signals_df.final_signal.iloc[-1] == 1:
                trade_type = "Entry Long"
                perc_equity = trade_sizes_long[trade_conditions[-1]]
            elif signals_df.final_signal.iloc[-1] == -1:
                trade_type = "Entry Short"
                perc_equity = trade_sizes_short[trade_conditions[-1]]

            logger.info(
                f"Signal: {trade_type} | Perc Equity: {perc_equity} | Symbol: {self.symbol}"
            )

            systems_to_check = [x for x in self.systems if x.name != "research"]
            for system in systems_to_check:
                self.send_signal(
                    system=system,
                    strategy_name=self.strategy_name,
                    trade_type=trade_type,
                    perc_equity=perc_equity,
                )
