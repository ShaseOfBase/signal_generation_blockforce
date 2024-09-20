import asyncio
from pathlib import Path
import tomllib
import time
import traceback
import argparse
from typing import List

from config import SLACK_CHANNEL
from lib.dev_mode import set_dev_mode
from lib.logger import get_logger_instance
from lib.data_client import DataClient
from lib.strategy_client import StrategyClient
from lib.models import PortfolioConfig, StrategyConfig, SystemConfig
from strategy_clients.psar_client.psar_client import SignalGeneratorPsar
from strategy_clients.simple_strategies.simple_bbands_client import (
    SignalGeneratorSimpleBbands,
)
from strategy_clients.simple_strategies.simple_ma_client import (
    SignalGeneratorSimpleMa,
)
from strategy_clients.simple_strategies.simple_macd_client import (
    SignalGeneratorSimpleMacd,
)
from strategy_clients.simple_strategies.simple_rsi_divergence_client import (
    SignalGeneratorSimpleRsiDivergence,
)

logger = get_logger_instance(__name__)


def get_signal_generator(
    strategy_config: StrategyConfig,
    data_client: DataClient,
    systems_configs: List[SystemConfig],
) -> StrategyClient:

    strategy_type = strategy_config.meta.strategy_type

    # TODO - do this in a less hard-coded way
    if strategy_type == "psar":
        raise NotImplementedError("PSAR not implemented")
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

    logger = get_logger_instance(strategy_config.strategy_name)

    signal_gen_kwargs = dict(
        system_configs=systems_configs,
        strategy_name=strategy_config.strategy_name,
        data_client=data_client,
        symbol=strategy_config.meta.symbol,
        strategy_config=strategy_config,
        logger=logger,
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


async def main(system_names: List[str], pf_name: str):
    active_pf = PortfolioConfig.from_name(pf_name)

    system_configs = [
        SystemConfig.from_name(system_name) for system_name in system_names
    ]

    tasks = []
    for strategy_config_name, strategy_params in active_pf.strategy_regimes.items():
        logger.info(f"Initializing strategy {strategy_config_name}...")
        dc = DataClient(system_config=system_configs[0])
        strategy_config = StrategyConfig.from_name(strategy_config_name)
        strategy_config.set_strategy_params(strategy_params)
        signal_generator = get_signal_generator(strategy_config, dc, system_configs)

        # Schedule coroutines using asyncio.create_task() and add them to a task list
        tasks.append(asyncio.create_task(generate_signals(signal_generator)))

    # Wait for all tasks to complete concurrently
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    # Argument parsing outside async function for clarity
    parser = argparse.ArgumentParser()
    parser.add_argument("-dev", action="store_true")
    args = parser.parse_args()
    if args.dev:
        set_dev_mode(True)

    system_config_names = ["research"]
    pf_config_name = "pf_simple_strategies_w_regime"

    asyncio.run(main(system_config_names, pf_config_name))
