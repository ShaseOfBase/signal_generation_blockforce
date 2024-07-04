import logging
from pathlib import Path


def setup_logging(strategy_name: str):
    # Create a logger
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    log_folder = Path("logs")
    if not log_folder.exists():
        log_folder.mkdir(parents=True)

    log_path = log_folder / f"{strategy_name}.log"
    # Create a file handler for writing logs to a file
    file_handler = logging.FileHandler(log_path)
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


logger = logging.getLogger(__name__)
