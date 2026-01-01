import pandas as pd
import numpy as np
import pytest
from indicators.trend import linear_regression_slope


def test_linear_regression_slope_basic():
    # Perfect linear trend with slope = 2
    series = pd.Series([1, 3, 5, 7, 9])
    result = linear_regression_slope(series, window=3)

    # First two must be NaN
    assert result.iloc[0:2].isna().all()

    # Remaining slopes should be ~2
    assert np.isclose(result.iloc[2], 2.0)
    assert np.isclose(result.iloc[3], 2.0)
    assert np.isclose(result.iloc[4], 2.0)


def test_linear_regression_invalid_window():
    series = pd.Series([1, 2, 3])

    with pytest.raises(ValueError):
        linear_regression_slope(series, window=0)
