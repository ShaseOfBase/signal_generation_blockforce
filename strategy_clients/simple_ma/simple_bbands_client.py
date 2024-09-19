import logging
import tomllib
from typing import List
from config import SLACK_CHANNEL
from lib.data_client import DataClient
from lib.models import System
from lib.path_manager import PathManager
from lib.regimes.regime_manager import apply_regime_filter_to_trigger_data
from lib.strategy_client import StrategyClient


logger = logging.getLogger(__name__)


class SignalGeneratorSimpleBbands(StrategyClient):
    """
    Signal generator for the Simple Bollinger Bands strategy
    """

    def __init__(
        self,
        systems: List[System],
        strategy_name: str,
        data_client: DataClient,
        symbol: str,
        config_name: str,
    ):
        super().__init__(systems=systems)
        self.strategy_name = strategy_name
        self.data_client = data_client
        self.symbol = symbol
        self.accounts = systems
        self.df_1h = None
        self.stale_data = False
        self.config_name = config_name

        self.initialize_config()
        self.initialize_data()

    def initialize_config(self):
        strategy_configs_path = PathManager.strategy_configs_path

        with open(strategy_configs_path / f"{self.config_name}.toml", "rb") as f:
            strategy_config = tomllib.load(f)

        self.strategy_params = strategy_config.get("strategy", {})

    def initialize_data(self, retry_count=0):
        # We need 30 days of warmup data
        self.df_1h = self.data_client.get_historical_data(
            symbol=self.symbol,
            candle_length_minutes=60,
            number_of_candles=55,  # 55 is the slow MA length
        )

        self.df_1d = self.data_client.get_historical_data(
            symbol=self.symbol,
            candle_length_minutes=1440,
            number_of_candles=365,
        )

        data = [self.df_1h, self.df_1d]

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
        self.df_1h, stale_1h, updated_1h = self.data_client.update_candles(
            self.symbol, self.df_1h, 60, 5
        )

        self.df_1d, stale_1d, updated_1d = self.data_client.update_candles(
            self.symbol, self.df_1d, 1440, 5
        )

        go = False

        if stale_1h:
            self.stale_data = True
            msg = f"Stale Data in Update Data | 1m Bar {stale_1h}"
            self.send_message(self.strategy_name, msg, SLACK_CHANNEL)
            logger.error(msg)
        else:
            self.stale_data = False

        if updated_1h:
            go = True

        return go

    def generate_signal(self):
        go = self.update_data()

        if not go:
            return

        logger.info(f"Running signal generation for {self.strategy_name}")

        global_warmup_symbol_ohlcv_df = self.df_1h

        # Get the slow MA
        slow_ma = global_warmup_symbol_ohlcv_df.close.rolling(window=55).mean()

        # Get the fast MA
        fast_ma = global_warmup_symbol_ohlcv_df.close.rolling(window=10).mean()

        # Get the signal

        long_entries = None
        long_exits = None
        short_entries = None
        short_exits = None

        if "long" in self.strategy_params:
            long_entries = fast_ma.vbt.crossed_above(slow_ma)
            long_exits = fast_ma.vbt.crossed_below(slow_ma)

        if "short" in self.strategy_params:
            short_entries = fast_ma.vbt.crossed_below(slow_ma)
            short_exits = fast_ma.vbt.crossed_above(slow_ma)

        if "use_regime" in self.strategy_params:
            use_regime = self.strategy_params["use_regime"]
            if use_regime:
                entries = {}
                exits = {}
                if "long" in self.strategy_params["directions"]:
                    entries["long_entries"] = long_entries
                    exits["long_exits"] = long_exits
                if "short" in self.strategy_params["directions"]:
                    entries["short_entries"] = short_entries
                    exits["short_exits"] = short_exits

                long_entries, long_exits, short_entries, short_exits = (
                    apply_regime_filter_to_trigger_data(
                        regime_version="v1",
                        strategy_instance_params=self.strategy_params,
                        ffill_entries=False,
                        **entries,
                        **exits,
                    )
                )

        if long_entries is not None:
            final_long_entry = long_entries.iloc[-1]
        if short_entries is not None:
            final_short_entry = short_entries.iloc[-1]
        if long_exits is not None:
            final_long_exit = long_exits.iloc[-1]
        if short_exits is not None:
            final_short_exit = short_exits.iloc[-1]

        # TODO - handle conflicting signals?
        if final_long_entry:
            trade_type = "Entry Long"
        elif final_short_entry:
            trade_type = "Entry Short"
        elif final_long_exit:
            trade_type = "Exit Long"
        elif final_short_exit:
            trade_type = "Exit Short"
        else:
            trade_type = None

        if trade_type is not None:
            logger.info(f"{self.strategy_name} signal generated: {trade_type}")

            systems_to_check = [x for x in self.systems if x.name != "research"]
            for system in systems_to_check:
                self.send_signal(
                    system=system,
                    strategy_name=self.strategy_name,
                    trade_type=trade_type,
                    perc_equity=1,
                )
