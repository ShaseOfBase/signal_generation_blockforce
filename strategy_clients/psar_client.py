import logging
from typing import List

from config import SLACK_CHANNEL
from lib.data_client import DataClient
from lib.models import System
from lib.strategy_client import StrategyClient


logger = logging.getLogger(__name__)


class SignalGeneratorPsar(StrategyClient):
    """
    Signal generator for the Parabolic SAR strategy
    """

    def __init__(
        self,
        systems: List[System],
        strategy_name: str,
        data_client: DataClient,
        symbol: str,
        strategy_params: dict,
    ):
        super().__init__(systems=systems)
        self.strategy_name = strategy_name
        self.data_client = data_client
        self.symbol = symbol
        self.accounts = systems
        self.df_1m = None
        self.stale_data = False

        self.initialize_data()

    def initialize_data(self, retry_count=0):
        self.df_1m = self.data_client.get_historical_data(
            symbol=self.symbol, candle_length_minutes=1, number_of_candles=144
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
        self.df_1m, stale_1m, updated_1m = self.data_client.update_hour_bars(
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
