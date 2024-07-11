from pathlib import Path
import tomllib
import time
import traceback
import argparse

from config import RESEARCH_PG_URI, SLACK_CHANNEL
from lib.dev_mode import set_dev_mode
from lib.logger import setup_logging
from lib.logger import logger
from lib.data_client import DataClient
from lib.strategy_client import StrategyClient
from lib.models import System
from strategy_clients.psar_client.psar_client import SignalGeneratorPsar


research = System(name="research", db_url=RESEARCH_PG_URI)


def get_psar_signal_generator(symbol: str) -> StrategyClient:
    systems = [
        research,
    ]

    dc = DataClient(systems=systems)

    # strategy_name = "Big Bend BTC"
    # big_bend_signal_generator_btc = SignalGeneratorBigBend(
    #     systems=systems,
    #     strategy_name=strategy_name,
    #     data_client=dc,
    #     symbol="BTCUSDT",
    # )

    strategy_name = "Parabolic SAR"
    setup_logging(strategy_name=strategy_name)

    psar_signal_generator_btc = SignalGeneratorPsar(
        systems=systems,
        strategy_name=strategy_name,
        data_client=dc,
        symbol=symbol,
    )

    return psar_signal_generator_btc


def generate_signals(signal_generator: StrategyClient):

    while True:
        try:
            signal_generator.generate_signal()
            time.sleep(10)
        except KeyboardInterrupt:
            print("Exiting loop due to user interruption.")
            break
        except Exception as e:
            logger.error(traceback.format_exc())
            signal_generator.send_message(
                "General Error",
                f"Error in main loop: {e}",
                SLACK_CHANNEL,
            )


if __name__ == "__main__":
    args = argparse.ArgumentParser()
    args.add_argument("-dev", action="store_true")
    args = args.parse_args()

    if args.dev:
        set_dev_mode(True)

    psar_signal_generator_btc = get_psar_signal_generator(symbol="BTCUSDT")

    generate_signals(psar_signal_generator_btc)
