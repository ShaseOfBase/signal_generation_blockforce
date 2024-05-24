import logging

import pandas as pd
from pandas import DataFrame
from config import SLACK_CHANNEL
from strategy_clients.data_client import DataClient
from strategy_clients.strategy_client import StrategyClient

logger = logging.getLogger(__name__)


class SignalGeneratorBigBend(StrategyClient):
    """
    This class should generate the signals for the low vol martingale strategy (big bend)

    What we need:

    50 4 hour candles
    200 90 mill dollar bars
    6 2 hour candles
    """

    def __init__(
        self, systems: dict, strategy_name: str, data_client: DataClient, symbol: str
    ):
        super().__init__(systems=systems)
        self.strategy_name = strategy_name
        self.data_client = data_client
        self.symbol = symbol
        self.accounts = systems
        self.df_4h = None
        self.df_db = None
        self.stale_data = False

        self.initialize_data()

    def initialize_data(self):
        self.df_4h = self.data_client.get_historical_data(
            symbol=self.symbol, candle_length_minutes=4*60, number_of_candles=51
        )
        self.df_2h = self.data_client.get_historical_data(
            symbol=self.symbol, candle_length_minutes=2*60, number_of_candles=7
        )
        self.df_db = self.data_client.get_historical_data_db(
            symbol=self.symbol, db_value=90_000_000, number_of_bars=201
        )

        data = [self.df_4h, self.df_2h, self.df_db]
        if any(item is None for item in data):
            # Not sure what to do here, probably need some mechanism to wait and then try again
            msg = "Historical data fetching failed - Exiting App"
            self.send_message(self.strategy_name, msg, SLACK_CHANNEL)
            logger.error("Historical Data Fetch Failed")
            exit()

    def update_data(self) -> bool:
        self.df_4h, stale_4h, updated_4h = self.data_client.update_hour_bars(
            self.symbol, self.df_4h, 4*60, 51
        )
        self.df_2h, stale_2h, updated_2h = self.data_client.update_hour_bars(
            self.symbol, self.df_2h, 2*60, 7
        )
        self.df_db, stale_db, updated_db = self.data_client.update_bars_db(
            self.df_db, self.symbol, 90_000_000, 201
        )

        go = False

        if stale_4h or stale_db or stale_2h:
            self.stale_data = True
            msg = f"Stale Data in Update Data | 4h Bar {stale_4h} 2h Bar {stale_2h} DB Bar {stale_db}"
            self.send_message(self.strategy_name, msg, SLACK_CHANNEL)
            logger.error(msg)
        else:
            self.stale_data = False

        if updated_4h or updated_db:
            logger.info(
                f"Updated Dollar Bar: {updated_db} | Updated 4h Bar {updated_4h}"
            )
            go = True

        return go

    def calculate_simple_moving_average(self, df: DataFrame, col: str, window: int):
        return df[col].rolling(window=window).mean()

    def calculate_exponential_moving_average(
        self, df: DataFrame, col: str, window: int
    ):
        return df[col].ewm(span=window, adjust=False).mean()

    def calculate_atr(self, df: DataFrame):
        return (df["high"] - df["low"]) / df["open"]

    def generate_signal(self):
        go = self.update_data()

        if not go:
            return

        df_db = self.df_db.copy()
        df_db["EMA50"] = self.calculate_exponential_moving_average(
            self.df_db, "duration", 50
        )
        df_db["SMA200"] = self.calculate_simple_moving_average(
            self.df_db, "duration", 200
        )

        latest_db = df_db.iloc[-1]
        second_latest_db = df_db.iloc[-2]

        df_4h = self.df_4h.copy()
        df_4h["SMA20"] = self.calculate_simple_moving_average(self.df_4h, "close", 20)
        df_4h["SMA50"] = self.calculate_simple_moving_average(self.df_4h, "close", 50)

        latest_4h = df_4h.iloc[-1]
        second_latest_4h = df_4h.iloc[-2]

        df_2h = self.df_2h.copy()
        df_2h["ATR"] = self.calculate_atr(df_2h)
        df_2h["ATR_AVG"] = self.calculate_simple_moving_average(df_2h, "ATR", 6)
        latest_2h = df_2h.iloc[-1]
        logger.info(f"{round(latest_2h['ATR_AVG'], 4)}")

        vol = "High Vol"
        if latest_db["EMA50"] > latest_db["SMA200"]:
            vol = "Low Vol"
            if second_latest_db["EMA50"] <= second_latest_db["SMA200"]:
                # Just moved to low vol
                self.send_message(
                    self.strategy_name, "Entering Low Vol Period", SLACK_CHANNEL
                )
        elif (
            latest_db["EMA50"] <= latest_db["SMA200"]
            and second_latest_db["EMA50"] > second_latest_db["SMA200"]
        ):
            # Just moved to high vol
            systems_to_check = [x for x in self.systems if x.name != "research"]
            for system in systems_to_check:
                self.send_signal(
                    system=system,
                    strategy_name=self.strategy_name,
                    trade_type="Exit Position",
                )
            self.send_message(
                self.strategy_name, "Entering High Vol Period", SLACK_CHANNEL
            )

        direction = None
        crossover = None
        if latest_4h["SMA20"] > latest_4h["SMA50"]:
            direction = "Entry Long"
            if second_latest_4h["SMA20"] <= second_latest_4h["SMA50"]:
                crossover = "Exit Short and Enter Long"

        if latest_4h["SMA20"] < latest_4h["SMA50"]:
            direction = "Entry Short"
            if second_latest_4h["SMA20"] >= second_latest_4h["SMA50"]:
                crossover = "Exit Long and Enter Short"

        if vol == "Low Vol":
            systems_to_check = [x for x in self.systems if x.name != "research"]


            if crossover is not None:
                # if any account in a system has a position, exit
                for system in systems_to_check:
                    self.send_signal(
                        system=system,
                        strategy_name=self.strategy_name,
                        trade_type="Exit Position",
                    )
                    self.send_message(
                        self.strategy_name,
                        f"{system}: Sending Exit Position SMA Crossover {crossover}",
                        SLACK_CHANNEL
                    )
            
            if crossover is None:
                if latest_2h["ATR_AVG"] < 0.008:
                    self.send_message(
                        self.strategy_name,
                        f"ATR Lower than 80bps {round(latest_2h['ATR_AVG'], 4)}",
                        SLACK_CHANNEL
                    )
                    self.send_message(
                        self.strategy_name,
                        f"{system}: Sending {direction}",
                        SLACK_CHANNEL
                    )

                    for system in systems_to_check:
                        self.send_signal(
                            system=system,
                            strategy_name=self.strategy_name,
                            trade_type=direction,
                        )

        logger.info(
            f"{vol}: {direction} | 4h SMA20 {round(latest_4h['SMA20'], 2)} SMA50 {round(latest_4h['SMA50'], 2)} | DB EMA50 {latest_db['EMA50']} SMA200 {latest_db['SMA200']}"
        )
