from pathlib import Path
import tomllib
import time
import traceback

from config import RESEARCH_PG_URI, SLACK_CHANNEL
from lib.logger import setup_logging
from lib.logger import logger
from lib.data_client import DataClient
from lib.strategy_client import StrategyClient
from lib.models import System
from strategy_clients.psar_client import SignalGeneratorPsar


research = System(name="research", db_url=RESEARCH_PG_URI)


def get_signal_generator() -> StrategyClient:
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

    with open(Path("strategy_configs/psar_config.toml"), "rb") as f:
        psar_config = tomllib.load(f)

    strategy_params = psar_config["strategy"]

    strategy_name = "Parabolic SAR"
    setup_logging(strategy_name=strategy_name)

    psar_signal_generator_btc = SignalGeneratorPsar(
        systems=systems,
        strategy_name=strategy_name,
        data_client=dc,
        symbol="BTCUSDT",
        strategy_params=strategy_params,
    )

    return psar_signal_generator_btc


def generate_signals(signal_generator: StrategyClient):
    sc = StrategyClient(systems=signal_generator.systems)

    while True:
        try:
            signal_generator.generate_signal()
            time.sleep(10)
        except KeyboardInterrupt:
            print("Exiting loop due to user interruption.")
            break
        except Exception as e:
            logger.error(traceback.format_exc())
            sc.send_message(
                "General Error",
                f"Error in main loop: {e}",
                SLACK_CHANNEL,
            )


if __name__ == "__main__":
    psar_signal_generator_btc = get_signal_generator()

    generate_signals(psar_signal_generator_btc)
