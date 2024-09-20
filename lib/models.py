from __future__ import annotations
import tomllib
from pydantic import BaseModel
from typing import Self, Optional, List, Dict
from dataclasses import dataclass

from lib.path_manager import PathManager


# This Schema is duplicated in pm_ee/pm_app/schemas/yosemite.py
class YosemiteSignalSchema(BaseModel):
    passphrase: Optional[str] = None
    strategy: str
    ticker: Optional[str] = None
    exchange: Optional[str] = None
    close: Optional[str] = None
    time: Optional[str] = None
    interval: Optional[str]
    type: str
    perc_equity: Optional[float] = None
    depth_threshold_ratio: Optional[float] = None
    depth_amount: Optional[int] = None
    slippage: Optional[float]
    text: Optional[str] = None

    force: Optional[bool] = False
    ignore_two_min_interval: Optional[bool] = False


class SystemConfig(BaseModel):
    name: str
    db_url: Optional[str] = None
    trading_url: Optional[str] = None

    @classmethod
    def from_name(cls, name: str) -> SystemConfig:
        system_config_path = PathManager.system_configs_path / f"{name}.toml"
        with open(system_config_path, "rb") as f:
            toml_data = tomllib.load(f)

            db_user = toml_data["db"]["user"]
            db_pass = toml_data["db"]["pass"]
            db_host = toml_data["db"]["host"]
            db_port = toml_data["db"]["port"]
            db_name = toml_data["db"]["name"]

            toml_data["db_url"] = (
                f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
            )

            toml_data.pop("db")

            return cls.model_validate(toml_data)


class PortfolioMeta(BaseModel):
    description: str


class PortfolioConfig(BaseModel):
    portfolio_meta: PortfolioMeta
    strategy: dict
    strategy_regimes: dict

    @classmethod
    def from_name(cls, name: str) -> PortfolioConfig:
        pf_config_path = PathManager.portfolio_configs_path / f"{name}.toml"
        with open(pf_config_path, "rb") as f:
            # Load toml file
            toml_data = tomllib.load(f)
            return cls.model_validate(toml_data)


class StrategyMeta(BaseModel):
    strategy_type: str
    timeframe: str
    symbol: str


class StrategyConfig(BaseModel):
    meta: StrategyMeta
    strategy: dict
    strategy_name: str
    file_name: Optional[str] = None

    def set_strategy_params(self, strategy_params: dict):
        self.strategy = {**self.strategy, **strategy_params}

    @classmethod
    def from_name(cls, strategy_name: str) -> StrategyConfig:
        strategy_config_path = (
            PathManager.strategy_configs_path / f"{strategy_name}.toml"
        )
        if not strategy_config_path.exists():
            raise FileNotFoundError(
                f"Strategy config file not found: {strategy_config_path}"
            )

        with open(strategy_config_path, "rb") as f:
            toml_data = tomllib.load(f)
            toml_data["file_name"] = strategy_config_path.stem
            toml_data["strategy_name"] = strategy_name

            return cls.model_validate(toml_data)
