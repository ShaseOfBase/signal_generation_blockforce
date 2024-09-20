import vectorbtpro as vbt
import logging
from typing import List
from config import SLACK_CHANNEL
from lib.data_client import DataClient
from lib.models import StrategyConfig, SystemConfig
from lib.path_manager import PathManager
from lib.regimes import regime_manager
from lib.regimes.regime_manager import apply_regime_filter_to_trigger_data
from lib.strategy_client import StrategyClient


class SignalGeneratorSimpleMacd(StrategyClient):
    """
    Signal generator for the Simple Moving Average strategy
    # Detect MACD divergence strategy signals for short only
    data['Signal_Sell'] = np.where((data['MACD'] < data['Signal']) & (data['MACD'].shift(1) >= data['Signal'].shift(1)), -1, 0)
    data['Signal_Buy'] = np.where((data['MACD'] > data['Signal']) & (data['MACD'].shift(1) <= data['Signal'].shift(1)), 1, 0)

    # Detect MACD divergence strategy signals for long only
    data['Signal_Buy'] = np.where((data['MACD'] > data['Signal']) & (data['MACD'].shift(1) <= data['Signal'].shift(1)), 1, 0)
    data['Signal_Sell'] = np.where((data['MACD'] < data['Signal']) & (data['MACD'].shift(1) >= data['Signal'].shift(1)), 1, 0)
    """

    def __init__(
        self,
        system_configs: List[SystemConfig],
        strategy_name: str,
        data_client: DataClient,
        symbol: str,
        strategy_config: StrategyConfig,
        logger: logging.Logger,
    ):
        super().__init__(
            system_configs=system_configs,
            strategy_name=strategy_name,
            data_client=data_client,
            symbol=symbol,
            strategy_config=strategy_config,
            logger=logger,
        )

        self.initialize_data()

    def initialize_data(self, retry_count=0):
        # We need 30 days of warmup data
        self.df_1h = self.data_client.get_historical_data(
            symbol=self.symbol,
            candle_length_minutes=60,
            number_of_candles=55,  # 55 is the slow MA length
            system_name=self.system_configs[0].name,
        )

        self.df_1d = self.data_client.get_historical_data(
            symbol=self.symbol,
            candle_length_minutes=1440,
            number_of_candles=365,
            system_name=self.system_configs[0].name,
        )

        data = [self.df_1h, self.df_1d]

        max_retries = 5
        if any(item is None for item in data) and retry_count < max_retries:
            msg = "Historical data fetching failed - retrying..."
            self.logger.info(msg)
            self.initialize_data(retry_count + 1)
        elif any(item is None for item in data):
            msg = (
                f"Historical data fetching failed for {self.strategy_name} "
                f"after {max_retries} retries - Exiting App"
            )
            self.logger.error(msg)
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
            self.logger.error(msg)
        else:
            self.stale_data = False

        if updated_1h:
            go = True

        if self.first_update:
            go = True
            self.first_update = False

        return go

    def generate_signal(self):

        go = self.update_data()

        if not go:
            self.logger.info("Data not updated, waiting....")
            return

        self.logger.info(f"Running signal generation for {self.strategy_name}")

        macd = vbt.MACD.run(
            self.df_1h.close, fast_window=12, slow_window=26, signal_window=9
        )

        macd_macd = macd.macd
        macd_signal = macd.signal

        long_entries = None
        long_exits = None
        short_entries = None
        short_exits = None

        if "long" in self.strategy_params["directions"]:
            long_entries = (macd_macd > macd_signal) & (
                macd_macd.shift(1) <= macd_signal.shift(1)
            )
            long_exits = (macd_macd < macd_signal) & (
                macd_macd.shift(1) >= macd_signal.shift(1)
            )

        if "short" in self.strategy_params["directions"]:
            short_entries = (macd_macd < macd_signal) & (
                macd_macd.shift(1) >= macd_signal.shift(1)
            )
            short_exits = (macd_macd > macd_signal) & (
                macd_macd.shift(1) <= macd_signal.shift(1)
            )

        # Get the signal
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

                long_entries, long_exits, short_entries, short_exits, regime_data = (
                    apply_regime_filter_to_trigger_data(
                        daily_ohlcv_df=self.df_1d,
                        trade_ohlcv_df=self.df_1h,
                        regime_version="v1",
                        strategy_instance_params=self.strategy_params,
                        ffill_entries=False,
                        **entries,
                        **exits,
                    )
                )
                current_regime_class = regime_data.regime_class.iloc[-1]
        else:
            current_regime_class = None

        final_long_entry = None
        final_short_entry = None
        final_long_exit = None
        final_short_exit = None

        if long_entries is not None:
            final_long_entry = long_entries.iloc[-1]
            # If the signal is a duplicate, don't trigger
            if final_long_entry == long_entries.iloc[-2]:
                final_long_entry = None

        if short_entries is not None:
            final_short_entry = short_entries.iloc[-1]
            if final_short_entry == short_entries.iloc[-2]:
                final_short_entry = None

        if long_exits is not None:
            final_long_exit = long_exits.iloc[-1]
            if final_long_exit == long_exits.iloc[-2]:
                final_long_exit = None

        if short_exits is not None:
            final_short_exit = short_exits.iloc[-1]
            if final_short_exit == short_exits.iloc[-2]:
                final_short_exit = None

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
            self.logger.info(f"{self.strategy_name} signal generated: {trade_type}")

            for system in self.system_configs:
                self.send_signal(
                    system=system,
                    strategy_name=self.strategy_name,
                    trade_type=trade_type,
                    perc_equity=1,
                    current_regime_class=current_regime_class,
                )