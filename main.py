import logging
import time
import traceback

from config import RESEARCH_PG_URI, SLACK_CHANNEL
from strategy_clients.big_bend_client import SignalGeneratorBigBend
from strategy_clients.data_client import DataClient
from strategy_clients.strategy_client import StrategyClient
from strategy_clients.models import System


def setup_logging():
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    # Create a file handler for writing logs to a file
    file_handler = logging.FileHandler("logs/big_bend_signal_generation.log")
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    file_handler.setFormatter(file_formatter)

    # Create a stream handler for writing logs to console
    stream_handler = logging.StreamHandler()
    stream_formatter = logging.Formatter("%(name)s - %(levelname)s - %(message)s")
    stream_handler.setFormatter(stream_formatter)

    # Add both handlers to the logger
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)


setup_logging()
logger = logging.getLogger(__name__)

research = System(name="research", db_url=RESEARCH_PG_URI)



def main():

    systems = [
        research,
    ]

    dc = DataClient(systems=systems)
    sc = StrategyClient(systems=systems)
    
    strategy_name = "Big Bend BTC"
    big_bend_signal_generator_btc = SignalGeneratorBigBend(
        systems=systems, strategy_name=strategy_name, data_client=dc, symbol="BTCUSDT"
    )

    while True:
        try:
            big_bend_signal_generator_btc.generate_signal()
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

    main()

