import asyncio
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
from lib.models import PortfolioConfig, StrategyConfig, System
from strategy_clients.psar_client.psar_client import SignalGeneratorPsar
from strategy_clients.simple_ma.simple_bbands_client import (
    SignalGeneratorSimpleBbands,
)
from strategy_clients.simple_ma.simple_ma_client import SignalGeneratorSimpleMa
from strategy_clients.simple_ma.simple_macd_client import SignalGeneratorSimpleMacd
from strategy_clients.simple_ma.simple_rsi_divergence_client import (
    SignalGeneratorSimpleRsiDivergence,
)


research = System(name="research", db_url=RESEARCH_PG_URI)


def get_signal_generator(strategy_config: StrategyConfig) -> StrategyClient:
    systems = [
        research,
    ]

    dc = DataClient(systems=systems)
    setup_logging(strategy_config.meta["strategy_type"])

    strategy_type = strategy_config.meta["strategy_type"]

    # TODO - do this in a less hard-coded way
    if strategy_type == "psar":
        signal_generator_cls = SignalGeneratorPsar
    elif strategy_type == "simple_rsi_divergence":
        signal_generator_cls = SignalGeneratorSimpleRsiDivergence
    elif strategy_type == "simple_macd":
        signal_generator_cls = SignalGeneratorSimpleMacd
    elif strategy_type == "simple_ma":
        signal_generator_cls = SignalGeneratorSimpleMa
    elif strategy_type == "simple_bbands":
        signal_generator_cls = SignalGeneratorSimpleBbands
    else:
        raise ValueError(f"Invalid strategy type: {strategy_type}")

    signal_gen_kwargs = dict(
        systems=systems,
        strategy_name=strategy_config.meta["strategy_type"],
        data_client=dc,
        symbol=strategy_config.meta["symbol"],
        config_name=strategy_config.file_name,
    )

    return signal_generator_cls(**signal_gen_kwargs)


async def generate_signals(signal_generator: StrategyClient):

    while True:
        try:
            await asyncio.to_thread(signal_generator.generate_signal)

            await asyncio.sleep(10)
        except KeyboardInterrupt:
            print("Exiting loop due to user interruption.")
            break
        except Exception as e:
            logger.error(traceback.format_exc())
            await asyncio.to_thread(
                signal_generator.send_message,
                "General Error",
                f"Error in main loop: {e}",
                SLACK_CHANNEL,
            )


async def main(dev_mode):
    if dev_mode:
        set_dev_mode(True)

    active_pf = PortfolioConfig.from_name("pf_simple_strategies_w_regime")

    tasks = []
    for strategy_config_name, strategy_params in active_pf.strategy_regimes.items():
        strategy_config = StrategyConfig.from_name(strategy_config_name)
        strategy_config.set_strategy_params(strategy_params)
        signal_generator = get_signal_generator(strategy_config)

        # Schedule coroutines using asyncio.create_task() and add them to a task list
        tasks.append(asyncio.create_task(generate_signals(signal_generator)))

    # Wait for all tasks to complete concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Argument parsing outside async function for clarity
    parser = argparse.ArgumentParser()
    parser.add_argument("-dev", action="store_true")
    args = parser.parse_args()

    # Pass dev_mode flag to main async function
    asyncio.run(main(args.dev))
