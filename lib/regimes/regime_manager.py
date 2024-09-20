import pandas as pd


def get_regime_data(regime_version, daily_ohlcv_df, trade_ohlcv_df):

    from lib.regimes.regime_v1 import RegimeV1

    if regime_version == "v1":
        regime = RegimeV1()
    else:
        raise ValueError(f"Invalid regime version {regime_version}")

    regime_data = regime.get_regime_df(daily_ohlcv_df, trade_ohlcv_df)

    return regime_data


def apply_regime_filter_to_trigger_data(
    daily_ohlcv_df: pd.DataFrame,
    trade_ohlcv_df: pd.DataFrame,
    regime_version: str,
    strategy_instance_params: dict,
    long_entries: pd.Series = None,
    long_exits: pd.Series = None,
    short_entries: pd.Series = None,
    short_exits: pd.Series = None,
    ffill_entries: bool = False,
):
    """
    Removes entries that are not in positive regimes,
    adds exits where we are in negative regimes.

    Turns all indices to the relevant value for the given regime, not
    just the index at the row where the regime shifts.
    """

    if long_entries is None and short_entries is None:
        raise ValueError("No short nor long entries provided")

    if ffill_entries:
        # ffill entries so that all positions where we should have entered previously are marked
        if long_entries is not None:
            long_entries = long_entries.ffill()
        if short_entries is not None:
            short_entries = short_entries.ffill()

    regime_data = get_regime_data(
        regime_version,
        daily_ohlcv_df,
        trade_ohlcv_df,
    )

    if long_entries is not None:

        long_entries_regime_df = pd.DataFrame(
            {
                "long_entries": long_entries,
                "regime_class": regime_data.regime_class.reindex(long_entries.index),
            },
        )
        # Change long entries to False if regime is not in bull classes from instance params
        long_entries_regime_df.loc[
            ~long_entries_regime_df.regime_class.isin(
                strategy_instance_params["regime_classes"]
            ),
            "long_entries",
        ] = False

        long_entries = long_entries_regime_df.long_entries

    if long_exits is not None:

        long_exits_regime_df = pd.DataFrame(
            {
                "long_exits": long_exits,
                "regime_class": regime_data.regime_class.reindex(long_exits.index),
            },
        )

        long_exits_regime_df.loc[
            ~long_exits_regime_df.regime_class.isin(
                strategy_instance_params["regime_classes"]
            ),
            "long_exits",
        ] = True

    if short_entries is not None:

        short_entries_regime_df = pd.DataFrame(
            {
                "short_entries": short_entries,
                "regime_class": regime_data.regime_class.reindex(
                    short_entries.index
                ),
            },
        )

        short_entries_regime_df.loc[
            ~short_entries_regime_df.regime_class.isin(
                strategy_instance_params["regime_classes"]
            ),
            "short_entries",
        ] = False

        short_entries = short_entries_regime_df.short_entries

    if short_exits is not None:

        short_exits_regime_df = pd.DataFrame(
            {
                "short_exits": short_exits,
                "regime_class": regime_data.regime_class.reindex(short_exits.index),
            },
        )

        short_exits_regime_df.loc[
            ~short_exits_regime_df.regime_class.isin(
                strategy_instance_params["regime_classes"]
            ),
            "short_exits",
        ] = True

    return long_entries, long_exits, short_entries, short_exits, regime_data
