import pandas as pd

def sma(series, window):

    """
    Parameters
    
    series : pandas.Series
        Time-ordered price series (e.g. close prices)
    window : int
        Lookback window size

    Returns
    
    pandas.Series
        SMA values aligned with input index
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    return series.rolling(window=window, min_periods=window).mean()




def ema(series, window):
    
    """
    Parameters
    
    series : pandas.Series
        Time-ordered price series
    window : int
        EMA lookback window

    Returns
    
    pandas.Series
        EMA values aligned with input index
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    return series.ewm(
        span=window,
        adjust=False,
        min_periods=window
    ).mean()
