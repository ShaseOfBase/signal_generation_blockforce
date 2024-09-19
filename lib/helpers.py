def get_annualized_volatility(data: pd.DataFrame, length_days: int):
    """
    Function assumes we are always working with daily data
    """

    ret_sr = data["Close"].pct_change()

    annualized_volatility = ret_sr.rolling(length_days).std() * (
        (365) ** 0.5
    )  # 30 day annualized volatility

    return annualized_volatility
