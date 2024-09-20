import numpy as np
import pandas as pd
from lib.helpers import get_annualized_volatility


class RegimeV1:

    def get_regime_df(
        self,
        daily_ohlcv_df: pd.DataFrame,
        trade_ohlcv_df: pd.DataFrame,
    ):
        """
                                Bull Trend	Sideways	Bear Trend
        Above Avg Volatility	Regime 1	Regime 3	Regime 5
        Below Avg Volatility	Regime 2	Regime 4	Regime 6

        Above Avg Volatility	21 day annualized volatility is HIGHER than the 365 day rolling average volatility
        Below Avg Volatility	21 day annualized volatility is LOWER than the 365 day rolling average volatility

        Bull Trend	Price ABOVE both 21 day moving average and 88 day moving average
        Sideways	Price BETWEEN both 21 day moving average and 88 day moving average
        Bear Trend	Price BELOW both 21 day moving average and 88 day moving average

        Returns:
            pd.DataFrame
            eg.
                            volatility | trend  |   volatility_class     | 	trend_class | regime class

                2021-01-01 	1.23	   |  1		|		Above Avg   	 | 	Bull        |     1
                2021-01-05	0.9		   |  0		|		Below Avg		 | 	Sideways    |     4
                2021-01-10	1.29	   |  -1	|		Above Avg		 |	Bear        |     5
        """

        fast_ann_vol = get_annualized_volatility(daily_ohlcv_df, 21)
        slow_ann_vol = get_annualized_volatility(daily_ohlcv_df, 365)

        regime_df = pd.DataFrame(index=daily_ohlcv_df.index)

        regime_df["volatility"] = fast_ann_vol / slow_ann_vol
        regime_df["volatility_class"] = np.where(
            regime_df["volatility"] > 1, "Above Avg", "Below Avg"
        )

        fast_ma = daily_ohlcv_df["close"].rolling(21).mean()
        slow_ma = daily_ohlcv_df["close"].rolling(88).mean()

        bull_conditions = (daily_ohlcv_df["close"] > fast_ma) & (
            daily_ohlcv_df["close"] > slow_ma
        )
        sideways_conditions = (
            (daily_ohlcv_df["close"] >= fast_ma)
            & (daily_ohlcv_df["close"] <= slow_ma)
        ) | (
            (daily_ohlcv_df["close"] <= fast_ma)
            & (daily_ohlcv_df["close"] >= slow_ma)
        )

        bear_conditions = (daily_ohlcv_df["close"] < fast_ma) & (
            daily_ohlcv_df["close"] < slow_ma
        )

        regime_df["trend_class"] = np.select(
            [bull_conditions, sideways_conditions, bear_conditions],
            ["Bull", "Sideways", "Bear"],
        )

        for row in regime_df.itertuples():
            if row.volatility_class == "Above Avg":
                if row.trend_class == "Bull":
                    regime_df.at[row.Index, "regime_class"] = 1
                elif row.trend_class == "Sideways":
                    regime_df.at[row.Index, "regime_class"] = 3
                elif row.trend_class == "Bear":
                    regime_df.at[row.Index, "regime_class"] = 5

            elif row.volatility_class == "Below Avg":
                if row.trend_class == "Bull":
                    regime_df.at[row.Index, "regime_class"] = 2
                elif row.trend_class == "Sideways":
                    regime_df.at[row.Index, "regime_class"] = 4
                elif row.trend_class == "Bear":
                    regime_df.at[row.Index, "regime_class"] = 6

        # Fill na with 0
        regime_df.regime_class = regime_df.regime_class.fillna(0)
        regime_df["regime_class"] = regime_df["regime_class"].astype(int)

        # Upsample daily back to original timeframe

        trade_freq = trade_ohlcv_df.index.to_series().diff().mode()[0]
        regime_df = regime_df.resample(trade_freq).ffill()

        return regime_df
