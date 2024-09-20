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


class SignalGeneratorSimpleRsiDivergence(StrategyClient):
    """
    Signal generator for the Simple Moving Average strategy

    RSI Divergence BTC	1H	If price makes a new 25 hour low, and RSI is below 30
    but not at 25 hour low then buy

    set TP and SL 2*ATR from entry

    RSI Divergence ETH	1H	If price makes a new 25 hour low, and RSI is below 30
    but not at 25 hour low then buy

    set TP and SL 2*ATR from entry
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

        rsi = vbt.RSI.run(self.df_1h.close, window=14).rsi
        atr = vbt.ATR.run(
            high=self.df_1h.high,
            low=self.df_1h.low,
            close=self.df_1h.close,
            window=14,
        ).atr

        long_entries = None
        long_exits = None
        short_entries = None
        short_exits = None

        if "long" in self.strategy_params["directions"]:
            long_entries = (
                (self.df_1h.close == self.df_1h.close.rolling(25).min())
                & (rsi < 30)
                & (rsi != rsi.rolling(25).min())
            )

        if "short" in self.strategy_params["directions"]:
            short_entries = (
                (self.df_1h.close == self.df_1h.close.rolling(25).max())
                & (rsi > 70)
                & (rsi != rsi.rolling(25).max())
            )

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

            if trade_type == "Entry Long":
                sl_stop = self.df_1h.close.iloc[-1] - 2 * atr.iloc[-1]
                tp_stop = self.df_1h.close.iloc[-1] + 2 * atr.iloc[-1]

            elif trade_type == "Entry Short":
                sl_stop = self.df_1h.close.iloc[-1] + 2 * atr.iloc[-1]
                tp_stop = self.df_1h.close.iloc[-1] - 2 * atr.iloc[-1]

            for system in self.system_configs:
                self.send_signal(
                    system=system,
                    strategy_name=self.strategy_name,
                    trade_type=trade_type,
                    perc_equity=1,
                    current_regime_class=current_regime_class,
                    sl_stop=sl_stop,
                    tp_stop=tp_stop,
                )
