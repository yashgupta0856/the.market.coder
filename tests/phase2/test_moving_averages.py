import pandas as pd
import numpy as np
import pytest
from indicators.moving_averages import sma
from indicators.moving_averages import ema


def test_sma_basic():
    series = pd.Series([1, 2, 3, 4, 5])
    result = sma(series, window=3)

    expected = pd.Series([np.nan, np.nan, 2.0, 3.0, 4.0])
    pd.testing.assert_series_equal(result, expected)


def test_sma_window_equals_one():
    series = pd.Series([10, 20, 30])
    result = sma(series, window=1)

    pd.testing.assert_series_equal(result, series.astype(float))


def test_sma_invalid_window():
    series = pd.Series([1, 2, 3])

    with pytest.raises(ValueError):
        sma(series, window=0)


def test_sma_short_series():
    series = pd.Series([1, 2])
    result = sma(series, window=5)

    assert result.isna().all()







def test_ema_basic():
    series = pd.Series([1, 2, 3, 4, 5])
    result = ema(series, window=3)

    # First two values must be NaN
    assert result.iloc[0:2].isna().all()
    assert not pd.isna(result.iloc[2])


def test_ema_window_one():
    series = pd.Series([10, 20, 30])
    result = ema(series, window=1)

    pd.testing.assert_series_equal(result, series.astype(float))


def test_ema_invalid_window():
    series = pd.Series([1, 2, 3])

    with pytest.raises(ValueError):
        ema(series, window=0)
