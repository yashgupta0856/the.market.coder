import pandas as pd
import numpy as np

def true_range(high, low, close):

    """
    Parameters
    
    high : pandas.Series
    low : pandas.Series
    close : pandas.Series

    Returns
    
    pandas.Series
        True Range values aligned to index
    """


    if not all(isinstance(x, pd.Series) for x in [high, low, close]):
        raise TypeError("high, low, close must be pandas Series")

    if not high.index.equals(low.index) or not high.index.equals(close.index):
        raise ValueError("high, low, close must have the same index")

    prev_close = close.shift(1)

    tr = pd.concat([
        high - low,
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)


    return tr





def atr(high, low, close, window):

    """
    Parameters
    
    high : pandas.Series
    low : pandas.Series
    close : pandas.Series
    window : int

    Returns
    
    pandas.Series
        ATR values aligned with input index
    """

    
    if window <= 0:
        raise ValueError("window must be a positive integer")

    tr = true_range(high, low, close)

    atr_series = tr.ewm(
        span=window,
        adjust=False,
        min_periods=window
    ).mean()

    return atr_series



def rolling_std(series, window):

    """
    Parameters
    
    series : pandas.Series
    window : int

    Returns
    
    pandas.Series
        Rolling standard deviation
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    return series.rolling(
        window=window,
        min_periods=window
    ).std()




def range_compression(high, low, close, short_window=20, long_window=60):

    """
    Returns
    
    pandas.Series
        Range compression ratio
    """

    if short_window <= 0 or long_window <= 0:
        raise ValueError("windows must be positive")

    if short_window >= long_window:
        raise ValueError("short_window must be < long_window")

    tr = true_range(high, low, close)

    recent = tr.rolling(
        window=short_window,
        min_periods=short_window
    ).mean()

    prior = tr.shift(short_window).rolling(
        window=long_window - short_window,
        min_periods=long_window - short_window
    ).mean()

    return recent / prior