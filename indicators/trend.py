import numpy as np
import pandas as pd

def linear_regression_slope(series, window):

    """
    Parameters
    
    series : pandas.Series
        Price series (usually close)
    window : int
        Lookback window size

    Returns
    
    pandas.Series
        Rolling linear regression slope
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    x = np.arange(window)

    def slope(y):
        if np.any(np.isnan(y)):
            return np.nan
        # polyfit returns [slope, intercept]
        return np.polyfit(x, y, 1)[0]

    return series.rolling(window=window, min_periods=window).apply(
        slope,
        raw=True
    )