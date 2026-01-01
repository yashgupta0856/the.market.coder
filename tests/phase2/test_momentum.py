import pandas as pd
import numpy as np
import pytest
from indicators.momentum import roc


def test_roc_basic():
    series = pd.Series([10, 20, 30, 40, 50])
    result = roc(series, window=2)

    expected = pd.Series([
        np.nan,
        np.nan,
        (30 - 10) / 10 * 100,
        (40 - 20) / 20 * 100,
        (50 - 30) / 30 * 100,
    ])

    pd.testing.assert_series_equal(result, expected)


def test_roc_divide_by_zero():
    series = pd.Series([0, 10, 20])
    result = roc(series, window=1)

    assert pd.isna(result.iloc[1])


def test_roc_invalid_window():
    series = pd.Series([1, 2, 3])

    with pytest.raises(ValueError):
        roc(series, window=0)
