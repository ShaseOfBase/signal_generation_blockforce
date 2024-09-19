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


@dataclass
class System:
    name: str
    db_url: str = None
    trading_url: str = None
    accounts: List[str] = None


class PortfolioMeta(BaseModel):
    description: str
    warmup_period_days: int
    leverage: float


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


class StrategyConfig(BaseModel):
    meta: dict
    strategy: dict
    file_name: Optional[str] = None

    def set_strategy_params(self, strategy_params: dict):
        self.strategy = {**self.strategy, **strategy_params}

    @classmethod
    def from_name(cls, name: str) -> StrategyConfig:
        strategy_config_path = PathManager.strategy_configs_path / f"{name}.toml"
        if not strategy_config_path.exists():
            raise FileNotFoundError(
                f"Strategy config file not found: {strategy_config_path}"
            )
        with open(strategy_config_path, "rb") as f:
            toml_data = tomllib.load(f)
            toml_data["file_name"] = strategy_config_path.stem
            return cls.model_validate(toml_data)
