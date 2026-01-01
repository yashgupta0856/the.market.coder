import pandas as pd
import numpy as np
import pytest
from indicators.volatility import true_range,atr,rolling_std,range_compression


def test_true_range_basic():
    high = pd.Series([10, 12, 15])
    low = pd.Series([8, 11, 14])
    close = pd.Series([9, 10, 13])

    tr = true_range(high, low, close)

    expected = pd.Series([
        2,              # 10 - 8
        max(12-11, abs(12-9), abs(11-9)),
        max(15-14, abs(15-10), abs(14-10))
    ])

    pd.testing.assert_series_equal(tr, expected.astype(float))


def test_true_range_index_mismatch():
    high = pd.Series([10, 12])
    low = pd.Series([8, 11])
    close = pd.Series([9, 10], index=[1, 2])

    with pytest.raises(ValueError):
        true_range(high, low, close)





def test_atr_basic():
    high = pd.Series([10, 12, 15, 14])
    low = pd.Series([8, 11, 14, 13])
    close = pd.Series([9, 10, 13, 14])

    result = atr(high, low, close, window=3)

    # First two values must be NaN
    assert result.iloc[0:2].isna().all()
    assert not pd.isna(result.iloc[2])


def test_atr_invalid_window():
    high = pd.Series([10, 12])
    low = pd.Series([8, 11])
    close = pd.Series([9, 10])

    with pytest.raises(ValueError):
        atr(high, low, close, window=0)



def test_rolling_std_basic():
    series = pd.Series([1, 2, 3, 4, 5])
    result = rolling_std(series, window=3)

    assert result.iloc[0:2].isna().all()
    assert result.iloc[2] > 0




def test_range_compression_basic():
    high = pd.Series(range(100, 160))
    low = high - 2
    close = high - 1

    rc = range_compression(high, low, close)

    assert rc.isna().iloc[:59].all()
    assert rc.iloc[-1] > 0