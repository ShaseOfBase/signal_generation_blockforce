from datetime import timedelta
import numpy as np
import pandas as pd


def get_raw_psar_df(
    data_df, start=0.02, increment=0.02, maximum=0.2
) -> pd.DataFrame:
    high = data_df["High"]
    low = data_df["Low"]
    close = data_df["Close"]

    uptrend = np.full(len(data_df), np.nan)
    ep = np.full(len(data_df), np.nan)
    sar = np.full(len(data_df), np.nan)
    af = np.full(len(data_df), np.nan)
    next_bar_sar = np.full(len(data_df), np.nan)

    for i in range(len(data_df)):
        if i == 0:
            # print("first it")
            uptrend[i] = np.nan
            ep[i] = np.nan
            sar[i] = np.nan
            af[i] = start
            next_bar_sar[i] = np.nan
        else:
            first_trend_bar = False
            nbs_index = i - 1
            sar[i] = next_bar_sar[nbs_index]
            # print(f"af {af[i]} ep {ep[i]}")

            if i == 1:
                prev_sar = np.nan
                prev_ep = np.nan
                low_prev = low.iloc[i - 1]
                high_prev = high.iloc[i - 1]
                close_cur = close.iloc[i]
                close_prev = close.iloc[i - 1]

                if close_cur > close_prev:
                    uptrend[i] = True
                    ep[i] = high.iloc[i]
                    prev_sar = low_prev
                    prev_ep = high.iloc[i]
                else:
                    uptrend[i] = False
                    ep[i] = low.iloc[i]
                    prev_sar = high_prev
                    prev_ep = low.iloc[i]

                first_trend_bar = True
                sar[i] = prev_sar + start * (prev_ep - prev_sar)
                af[i] = af[i - 1]
            else:
                ep[i] = ep[i - 1]
                af[i] = af[i - 1]
                uptrend[i] = uptrend[i - 1]

            if uptrend[i]:
                if sar[i] > low.iloc[i]:
                    first_trend_bar = True
                    uptrend[i] = False
                    sar[i] = max(ep[i], high.iloc[i])
                    ep[i] = low.iloc[i]
                    af[i] = start
            else:
                if sar[i] < high.iloc[i]:
                    first_trend_bar = True
                    uptrend[i] = True
                    sar[i] = min(ep[i], low.iloc[i])
                    ep[i] = high.iloc[i]
                    af[i] = start

            if not first_trend_bar:
                if uptrend[i]:
                    if high.iloc[i] > ep[i]:
                        ep[i] = high.iloc[i]
                        af[i] = min(af[i] + increment, maximum)
                else:
                    if low.iloc[i] < ep[i]:
                        ep[i] = low.iloc[i]
                        af[i] = min(af[i] + increment, maximum)

            if uptrend[i]:
                sar[i] = min(sar[i], low.iloc[i - 1])
                if i > 1:
                    sar[i] = min(sar[i], low.iloc[i - 2])
            else:
                sar[i] = max(sar[i], high.iloc[i - 1])
                if i > 1:
                    sar[i] = max(sar[i], high.iloc[i - 2])

            next_bar_sar[i] = sar[i] + af[i] * (ep[i] - sar[i])
            # print(f"{i}: Sar {sar[i]} | Next Bar Sar {next_bar_sar[i]}")

    # data['PSAR_EP'] = ep
    data_df["PSAR"] = sar
    # data['PSAR_AF'] = af
    data_df["PSAR_NextBar"] = next_bar_sar
    data_df["PSAR_Uptrend"] = uptrend

    return data_df


def get_clean_psar_signals_dynamic_df(df, window=2) -> pd.DataFrame:
    def _zone_generator(start_time, window):
        hour_value = start_time.hour

        if hour_value < window:
            zone_start = start_time.replace(
                hour=0, minute=0, second=0, microsecond=0
            )
        elif hour_value % window == 0:
            zone_start = start_time.replace(minute=0, second=0, microsecond=0)
        else:
            hour_start = (hour_value // window) * window
            zone_start = start_time.replace(
                hour=hour_start, minute=0, second=0, microsecond=0
            )

        return zone_start

    df_signal = df[df["Signal"] != 0]
    df_signal = df_signal.reset_index()
    # df_signal.to_csv('cleaning.csv')

    start_time = df_signal.at[0, "datetime"]

    zone_start = _zone_generator(start_time, window)

    for index, row in df_signal.iterrows():
        if index == 0:
            continue

        if row["datetime"] > zone_start and row["datetime"] < (
            zone_start + timedelta(hours=window)
        ):
            df_signal.at[index, "Signal"] = 0
        else:
            zone_start = _zone_generator(row["datetime"], window)

    df_signal = df_signal[["datetime", "Signal"]]

    df = df[["datetime", "Open", "High", "Low", "Close"]]
    cleaned = pd.merge(
        df, df_signal, left_on="datetime", right_on="datetime", how="left"
    )
    cleaned["Signal"] = cleaned["Signal"].fillna(0)

    return cleaned


def get_cleaned_psar_df(
    minute_candles_df: pd.DataFrame,
    window=2,
    start=0.02,
    increment=0.02,
    maximum=0.2,
) -> pd.DataFrame:

    # Add datetime column matching index
    minute_candles_df["datetime"] = minute_candles_df.index

    # rename open to Open, close to Close etc...

    minute_candles_df = minute_candles_df.rename(
        columns={
            "open": "Open",
            "high": "High",
            "low": "Low",
            "close": "Close",
            "volume": "Volume",
        }
    )

    two_hour_candle_df = minute_candles_df.resample(f"{window}h").agg(
        {
            "Open": "first",
            "High": "max",
            "Low": "min",
            "Close": "last",
        }
    )

    raw_psar_df = get_raw_psar_df(two_hour_candle_df, start, increment, maximum)
    # TODO: compare with 3rd party PSAR indicators

    raw_psar_df = raw_psar_df[["PSAR", "PSAR_NextBar", "PSAR_Uptrend"]]

    # Merge 2-hour PSAR data into the minutely data using 'merge_asof' with 'direction=forward'
    merged_data_df = pd.merge_asof(
        minute_candles_df,
        raw_psar_df,
        left_index=True,
        right_index=True,
        direction="backward",
    )

    # merged_data['PSAR'].fillna(method='ffill', inplace=True)

    merged_data_df["PSAR_NextBar"] = merged_data_df["PSAR_NextBar"].ffill()
    merged_data_df["PSAR_Uptrend"] = merged_data_df["PSAR_Uptrend"].ffill()

    merged_data_df.reset_index(inplace=True)

    # Generate the 1 and -1
    merged_data_df["Signal"] = 0
    merged_data_df["PSAR_NextBar"] = merged_data_df["PSAR_NextBar"].shift(
        60 * window
    )

    merged_data_df.loc[
        (merged_data_df["PSAR_Uptrend"] == 1)
        & (merged_data_df["PSAR_NextBar"] < merged_data_df["High"])
        & (merged_data_df["PSAR_NextBar"] > merged_data_df["Low"]),
        "Signal",
    ] = 1  # Long Entry
    merged_data_df.loc[
        (merged_data_df["PSAR_Uptrend"] == 0)
        & (
            (merged_data_df["PSAR_NextBar"] < merged_data_df["High"])
            & (merged_data_df["PSAR_NextBar"] > merged_data_df["Low"])
        ),
        "Signal",
    ] = -1  # Short Ent

    cleaned_df = get_clean_psar_signals_dynamic_df(merged_data_df, window)

    cleaned_df = cleaned_df.set_index("datetime")

    return cleaned_df


def add_market_metrics(
    signals_df: pd.DataFrame,
    rolling_ret_period: int,
    mt_len: int,  # vol length - 30 Day
    rolling_peak_valley_period: int,
) -> pd.DataFrame:

    signals_df["ret"] = signals_df["Close"].pct_change()
    signals_df["rol_ret"] = signals_df["Close"].pct_change(
        periods=rolling_ret_period
    )  # rolling 30min (default) return

    signals_df["mt_vol"] = signals_df["ret"].rolling(mt_len).std() * (
        (365 * 24 * 60) ** 0.5
    )  # 30 day annualized volatility

    signals_df["rolling_peak"] = (
        signals_df["Close"].rolling(rolling_peak_valley_period).max()
    )  # last 24 hour (default) highest high price
    signals_df["rolling_valley"] = (
        signals_df["Close"].rolling(rolling_peak_valley_period).min()
    )  # last 24 hour (default) lowest low price
    signals_df["max_chg"] = (
        signals_df["rolling_peak"] / signals_df["rolling_valley"] - 1
    )  # peak to valley chg in percentage

    return signals_df


def add_final_signal(
    signals_df: pd.DataFrame,
    rolling_return_threshold: float,
    peak_valley_chg_threshold: float,
) -> pd.DataFrame:
    """
    We only trade on a signal only if
    1. 'rol_ret' is under 'rolling_return_under_threshold'
    2. 'max_chg' is under 'peak_valley_chg_threshold'

    So we set final signal to 1 or -1 only if the above conditions are met.

    We set trade_condition to high_vol or low_vol

    """

    signals_df["final_signal"] = np.where(
        (abs(signals_df["rol_ret"]) < rolling_return_threshold)
        & (signals_df["max_chg"] < peak_valley_chg_threshold)
        & (signals_df["Signal"] != 0)
        & (~np.isnan(signals_df["mt_vol"])),
        signals_df["Signal"],
        0,
    )

    return signals_df


def add_rolling_return_under_threshold(
    signals_df: pd.DataFrame, rolling_return_threshold: float
) -> pd.DataFrame:
    """
    Add a "rolling_ret_under_threshold" column to the signals dataframe
    that is 1 if the rolling return is under the threshold, else 0.
    """

    signals_df["rolling_ret_under_threshold"] = np.where(
        abs(signals_df["rol_ret"]) < rolling_return_threshold, 1, 0
    )

    return signals_df


def add_annualized_vol_under_threshold(
    signals_df: pd.DataFrame, high_vol_threshold: float
) -> pd.DataFrame:
    """
    Add a "annualized_vol_under_threshold" column to the signals dataframe
    that is 1 if the annualized volatility is under the threshold, else 0.
    """

    signals_df["annualized_vol_under_threshold"] = np.where(
        signals_df["mt_vol"] < high_vol_threshold, 1, 0
    )

    return signals_df


def add_trade_conditions(signals_df: pd.DataFrame) -> pd.DataFrame:
    """
    When 30 day (default) annualized volatility is above high_vol_threshold,
    we set trade_condition to high_vol, else we set it to low_vol.
    """

    signals_df["trade_condition"] = np.where(
        signals_df["annualized_vol_under_threshold"],
        "low_vol",
        "high_vol",
    )

    return signals_df


def get_psar_signals_df(
    symbol_ohlc_df,
    trial_instance_params,
    psar_settings,
):
    """
    Gets the "cleaned" PSAR signals DF from original backtesting.py logic,
    adds the market metrics to it and then expands on it further by
    adding final signal details and trade conditions.
    """

    processed_psar_df = get_cleaned_psar_df(
        minute_candles_df=symbol_ohlc_df, **psar_settings
    )

    processed_psar_df = add_market_metrics(
        processed_psar_df,
        rolling_ret_period=trial_instance_params["rolling_ret_period"],
        mt_len=trial_instance_params["mt_len"],
        rolling_peak_valley_period=trial_instance_params[
            "rolling_peak_valley_period"
        ],
    )

    processed_psar_df = add_final_signal(
        processed_psar_df,
        rolling_return_threshold=trial_instance_params["rolling_ret_thresh"],
        peak_valley_chg_threshold=trial_instance_params["peak_valley_chg_thresh"],
    )

    processed_psar_df = add_annualized_vol_under_threshold(
        processed_psar_df, trial_instance_params["vol_thresh"]
    )

    processed_psar_df = add_trade_conditions(processed_psar_df)

    processed_psar_df = add_rolling_return_under_threshold(
        processed_psar_df,
        rolling_return_threshold=trial_instance_params["rolling_ret_thresh"],
    )

    return processed_psar_df


def get_annualized_vol_under_threshold(symbol_ohlc_df, trial_instance_params):

    instance_symbol_ohlc_df = symbol_ohlc_df.copy()

    ret_sr = instance_symbol_ohlc_df["Close"].pct_change()

    mt_len = trial_instance_params["mt_len"]
    mt_vol_sr = ret_sr.rolling(mt_len).std() * (
        (365 * 24 * 60) ** 0.5
    )  # 30 day annualized volatility

    high_vol_threshold = trial_instance_params["vol_thresh"]
    annualized_vol_under_threshold = np.where(mt_vol_sr < high_vol_threshold, 1, 0)

    annualized_vol_under_threshold_sr = pd.Series(
        annualized_vol_under_threshold, index=instance_symbol_ohlc_df.Close.index
    )

    annualized_vol_under_threshold_sr.index = (
        instance_symbol_ohlc_df.index.tz_localize("UTC")
    )

    return annualized_vol_under_threshold_sr
