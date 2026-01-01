import pandas as pd
import numpy as np

def roc(series, window):

    """
    Parameters
    
    series : pandas.Series
        Price series (usually close)
    window : int
        Lookback window

    Returns
    
    pandas.Series
        ROC percentage values
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    shifted = series.shift(window)

    roc_series = (series - shifted) / shifted * 100

    roc_series = roc_series.replace([np.inf, -np.inf], np.nan)

    return roc_series