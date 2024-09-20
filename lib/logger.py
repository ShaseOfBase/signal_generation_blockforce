import logging
from pathlib import Path


# Create a default / base logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

log_folder = Path("logs")
if not log_folder.exists():
    log_folder.mkdir(parents=True)

log_path = log_folder / f"base.log"
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


def get_logger_instance(strategy_name: str) -> logging.Logger:
    # Create a logger for the strategy
    logger = logging.getLogger(strategy_name)
    logger.setLevel(logging.INFO)

    # Ensure that we don't add duplicate handlers
    if not logger.hasHandlers():
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

        # Create a stream handler for writing logs to the console
        stream_handler = logging.StreamHandler()
        stream_formatter = logging.Formatter(
            "%(name)s - %(levelname)s - %(message)s"
        )
        stream_handler.setFormatter(stream_formatter)

        # Add both handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

    return logger
