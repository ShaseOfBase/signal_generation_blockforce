# Signal Generation Repository

This repository implements signal generators for algorithmic trading strategies, focusing on real-time signal generation and integration with an algorithmic trading engine.

## Contents

- `big_bend_client.py`: Implements the `SignalGeneratorBigBend` class.
- `data_client.py`: Infrastructure for data fetching.
- `strategy_client.py`: Infrastructure for interacting with trading engine.
- `models.py`: Contains models and data structures.
- `main.py`: Main entry point for running the signal generation.

## SignalGeneratorBigBend

### Overview

`SignalGeneratorBigBend` is a class that implements a trading strategy based on low volatility martingale. It fetches historical data, updates it regularly, and generates trading signals based on the strategy's logic.

### Key Components

#### Initialization

- Fetches necessary historical data.
- Initializes dataframes for 4-hour candles, 2-hour candles, and dollar bars.

#### Data Fetching

- `initialize_data`: Fetches initial sets of historical data using `DataClient`.
- Ensures all required data is available; if not, it logs an error and exits.

#### Data Updating

- `update_data`: Updates the historical data and checks for stale data.
- If data is stale, it sends a notification and logs an error.

#### Signal Generation

- `generate_signal`: The core method where the strategy logic is implemented.
- Runs in a loop (every 10 seconds).
- Updates data and, if changes are detected, applies the strategy logic to generate signals.
