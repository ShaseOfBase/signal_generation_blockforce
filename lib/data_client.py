import datetime
import logging
import time
import traceback
import typing

import pandas as pd
from pandas import DataFrame
from sqlalchemy import create_engine, text
from sqlalchemy.exc import OperationalError
from sqlalchemy.orm import sessionmaker
from lib.models import System

logger = logging.getLogger(__name__)


class DataBaseClient:
    def __init__(self, systems: typing.List[System]):
        self.systems = systems
        self._init_db_connections()

    def _init_db_connections(self):
        self.db_handler = {}

        for x in self.systems:
            engine = create_engine(x.db_url, pool_pre_ping=True)
            session_maker = sessionmaker(bind=engine)
            self.db_handler[x.name] = {
                "session_maker": session_maker,
                "session": None,
            }

        logger.info("Initialized DB Connections")

    def get_session(self, session_name: str):
        session = self.db_handler[session_name]["session"]
        if session is None or not self.is_session_alive(session_name):
            self.db_handler[session_name]["session"] = self.db_handler[session_name][
                "session_maker"
            ]()

        return self.db_handler[session_name]["session"]

    def is_session_alive(self, session_name: str):
        session = self.db_handler[session_name]["session"]
        try:
            session.execute(text("SELECT 1"))
            return True
        except OperationalError:
            session.rollback()
            return False


class DataClient(DataBaseClient):
    def __init__(self, systems: dict):
        super().__init__(systems)
        self.last_update_time = None
        self.stale_threshold_seconds = 120  # two minute late data is stale

    ###### Time Based Bars #######

    def get_historical_data(
        self, symbol: str, candle_length_minutes: int, number_of_candles: int
    ) -> DataFrame:
        try:

            session = self.get_session("research")

            # If you want hourly candles, then we will use the 30m candles from DB to make
            if candle_length_minutes % 30 == 0:
                limit = (candle_length_minutes // 30) * number_of_candles + (
                    candle_length_minutes // 30
                )
                db_candle_length = 30
            else:
                limit = (
                    candle_length_minutes * number_of_candles + candle_length_minutes
                )
                db_candle_length = 1

            query = (
                f"SELECT * FROM candle WHERE symbol = '{symbol}' "
                f"AND kind = '{db_candle_length}m' ORDER BY close_datetime DESC LIMIT {limit}"
            )
            df = pd.read_sql(query, session.bind)

            stale_data = self.check_data_staleness(df, db_candle_length)
            if stale_data:
                logger.error("Data is stale")
                return None

            formatted_df = self.format_hour_bars(
                df,
                input_data_duration=db_candle_length,
                requested_data_duration=candle_length_minutes,
            )

            if len(formatted_df) >= number_of_candles:
                logger.info("Successfully Fetched Historical Data")
                self.last_update_time = datetime.datetime.now(
                    tz=datetime.timezone.utc
                )
            elif len(formatted_df) < number_of_candles or stale_data:
                logger.error("Failed to fetch enough Historical Data/stale data")
                # send slack message
                return None

            return formatted_df

        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            session.close()

    def check_data_staleness(self, df: DataFrame, db_candle_length: int) -> bool:
        """
        If the current time is greater than the last close time + candle_duration + stale_threshold_seconds, then the data is stale
        """
        df["close_datetime"] = pd.to_datetime(df["close_datetime"])
        df = df.sort_values(by="close_datetime")
        df["open_datetime"] = df["close_datetime"] - datetime.timedelta(
            minutes=db_candle_length
        )

        if df.empty:
            logger.warning(f"Dataframe is Empty")
            return True

        current_time = datetime.datetime.now(tz=datetime.timezone.utc)
        data_time = df["close_datetime"].iloc[-1]

        # logger.info(
        #     f"Current Time: {current_time} | Latest Close Data Point: {data_time}"
        # )
        if (
            current_time - data_time
        ).total_seconds() > db_candle_length * 60 + self.stale_threshold_seconds:
            logger.warning(f"Data for {df['symbol'].iloc[0]} is stale.")
            logger.warning(
                f"{(current_time - data_time).total_seconds()} > {30*60+ self.stale_threshold_seconds}"
            )
            # send slack message
            return True

        return False

    def format_hour_bars(
        self, df: DataFrame, input_data_duration: int, requested_data_duration: int
    ) -> DataFrame:

        df["close_datetime"] = pd.to_datetime(df["close_datetime"])
        df = df.sort_values(by="close_datetime")
        df["open_datetime"] = df["close_datetime"] - datetime.timedelta(
            minutes=input_data_duration
        )
        df = df.rename(columns={"open_datetime": "Open Time"})
        df = df.set_index("Open Time")

        # logger.info(f"Raw Data \n {df.to_markdown()}")

        ohlc_dict = {"open": "first", "high": "max", "low": "min", "close": "last"}

        hour_bars = df.resample(f"{requested_data_duration}min").agg(ohlc_dict)
        hour_bars.index.rename("Open Time", inplace=True)

        if hour_bars.index[0] != df.index[0]:
            # logger.info(f'Initial Mismatch {hour_bars.index[0]} != {df.index[0]}')
            hour_bars = hour_bars.drop(hour_bars.index[0])

        # If the last bar is not complete then remove it
        """
        candle duration = 2hours 
        Lets say this is the last values of the input
        8 2023-01-01 04:00:00      9
        9 2023-01-01 04:30:00     10

        Then we get this result
        2023-01-01 04:00:00    9

        This candle is not complete because for this candle to be complete 2023-01-01 04:00:00
        Then the last value in the input should be 2023-01-01 04:00:00 + 1.5 hours


        """

        if (
            hour_bars.index[-1]
            + datetime.timedelta(
                minutes=requested_data_duration - input_data_duration
            )
        ) != df.index[-1]:
            # logger.info("Removed the last bar")
            hour_bars = hour_bars.drop(hour_bars.index[-1])

        # logger.info(f"Formatted Data \n {hour_bars.to_markdown()}")
        return hour_bars

    def update_candles(
        self,
        symbol: str,
        df: DataFrame,
        candle_length_minutes: int,
        number_of_candles: int,
    ) -> typing.Tuple[DataFrame, bool, bool]:
        try:
            stale_data = False
            updated_data = False

            session = self.get_session("research")

            if candle_length_minutes % 30 == 0:
                limit = candle_length_minutes // 30
                db_candle_length = 30
            else:
                limit = candle_length_minutes
                db_candle_length = 1

            query = (
                f"SELECT * FROM candle WHERE symbol = '{symbol}' "
                f"AND kind = '{db_candle_length}m' ORDER BY close_datetime DESC LIMIT {limit}"
            )

            query_df = pd.read_sql(query, session.bind)

            # check if the data is stale
            stale_data = self.check_data_staleness(
                query_df, db_candle_length=db_candle_length
            )
            formatted_df = self.format_hour_bars(
                df=query_df,
                input_data_duration=db_candle_length,
                requested_data_duration=candle_length_minutes,
            )

            # is_different = not (df.tail(1).isin(formatted_df).all(axis=1).any())
            # Drop datetime to align with formatted_df
            if "datetime" in df.columns:
                df = df.drop(columns=["datetime"])
            is_different = not formatted_df.empty and not df.tail(1).equals(
                formatted_df
            )

            if not formatted_df.empty and not stale_data and is_different:
                df = pd.concat([df, formatted_df])
                logger.info("Added hour datapoint")
                self.last_update_time = datetime.datetime.now(
                    tz=datetime.timezone.utc
                )
                updated_data = True
                logger.info(f"Updated Data: {df.tail(1).to_markdown()}")
            else:
                pass
                # logger.info("Didn't add hour datapoint")

            if len(df) > number_of_candles:
                df = df.drop(df.index[0])
                # logger.info("Data is too long removing one")

            return df, stale_data, updated_data

        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            session.close()

    ###### Dollar Bars #######

    def check_data_staleness_db(self, symbol: str):
        """
        To check dollar bar staleness, we need to look at the source
        the dollar bars are created on
        """

        session = self.get_session("research")

        if symbol == "BTCUSDT":
            table_name = "btcusdt_usdm"
        elif symbol == "ETHUSDT":
            table_name = "ethusdt_usdm"

        query = text(
            f"SELECT close_time FROM {table_name} ORDER BY close_time DESC LIMIT 1"
        )
        result = session.execute(query).scalar()

        datetime_utc = datetime.datetime.utcfromtimestamp(result)
        now = datetime.datetime.utcnow()
        time_difference = now - datetime_utc
        # logger.info(f'Last {datetime_utc} Now {now} Diff {time_difference}')

        # Check if the given datetime is more than 5 minutes old
        is_older_than_5_minutes = time_difference > datetime.timedelta(minutes=5)

        return is_older_than_5_minutes

    def get_historical_data_db(
        self, symbol: str, db_value: int, number_of_bars: int
    ) -> DataFrame:
        try:
            session = self.get_session("research")
            # Use parameterized query for security and efficiency
            limit = number_of_bars
            query = f"SELECT * FROM gt_dollarbar WHERE symbol = '{symbol}' AND threshold = '{db_value}' ORDER BY close_time DESC LIMIT {limit}"
            df = pd.read_sql(query, session.bind)

            df = df.sort_values(by="open_time")
            if len(df) >= number_of_bars:
                logger.info("Successfully Fetched Historical Data DB")
            else:
                logger.error("Failed to fetch enough Historical Data/stale data DB")
                # send slack message
                return None
            return df
        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            session.close()

    def update_bars_db(
        self, df: DataFrame, symbol: str, db_value: int, number_of_bars: int
    ) -> typing.Tuple[DataFrame, bool, bool]:
        # Dollar Bars could come really fast so I just need to fetch the last couple and then add them
        try:
            stale_data = self.check_data_staleness_db(symbol=symbol)
            updated_data = False

            session = self.get_session("research")
            last_open_time = df["open_time"].iloc[-1]
            query = f"SELECT * FROM gt_dollarbar WHERE symbol = '{symbol}' AND threshold = '{db_value}' AND open_time > '{last_open_time}' ORDER BY open_time ASC"
            query_df = pd.read_sql(query, session.bind)

            if not query_df.empty:
                df = pd.concat([df, query_df])
                logger.info("Added DB datapoint")
                updated_data = True
            else:
                pass
                # logger.info("Didn't add DB datapoint")

            if len(df) > number_of_bars:
                df = df[-number_of_bars:]
                logger.info("Data is too long, truncated to the most recent bars")

            return df, stale_data, updated_data

        except Exception as e:
            logger.error(traceback.format_exc())
        finally:
            session.close()
