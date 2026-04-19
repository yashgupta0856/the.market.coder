import numpy as np
import pandas as pd


def linear_regression_slope(series, window):
    """
    Fully vectorised rolling linear regression slope.

    Uses the identity:
        slope = (n·Σxy − Σx·Σy) / (n·Σx² − (Σx)²)

    where x = [0, 1, …, n−1] within each window and the weighted sums
    are computed via two rolling sums, yielding O(len(series)) total work
    instead of O(len(series) × window) with np.polyfit.

    Parameters
    ----------
    series : pandas.Series
        Price series (usually close)
    window : int
        Lookback window size

    Returns
    -------
    pandas.Series
        Rolling linear regression slope
    """

    if window <= 0:
        raise ValueError("window must be a positive integer")

    if not isinstance(series, pd.Series):
        raise TypeError("series must be a pandas Series")

    n = window

    # Pre-computed constants for x = [0, 1, …, n-1]
    sum_x = n * (n - 1) / 2.0
    sum_x2 = n * (n - 1) * (2 * n - 1) / 6.0
    denom = n * sum_x2 - sum_x ** 2

    if denom == 0:
        return pd.Series(np.nan, index=series.index)

    y = series.astype(float)

    # Rolling Σy
    rolling_sum_y = y.rolling(window=n, min_periods=n).sum()

    # Global index array aligned to series
    idx = pd.Series(
        np.arange(len(series), dtype=float),
        index=series.index,
    )

    # Rolling Σ(global_j · y_j)
    rolling_sum_xy = (y * idx).rolling(window=n, min_periods=n).sum()

    # Convert global weighted sum to window-local weighted sum:
    # local_xy = rolling_sum_xy − (first_global_index_in_window) × rolling_sum_y
    offset = idx - n + 1
    local_xy = rolling_sum_xy - offset * rolling_sum_y

    slope = (n * local_xy - sum_x * rolling_sum_y) / denom

    return slope