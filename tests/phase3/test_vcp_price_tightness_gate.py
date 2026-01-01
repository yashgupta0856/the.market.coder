import pandas as pd
from scanners.vcp_scanner import price_tightness_gate


def test_price_tightness_gate_pass():
    df = pd.DataFrame({
        "close": [100, 101, 100.5, 100.8, 100.6] * 3,
        "high":  [101, 102, 101.2, 101.3, 101.1] * 3,
        "low":   [99, 100, 99.8, 100.1, 100.0] * 3,
    })

    assert price_tightness_gate(df, lookback=10) is True


def test_price_tightness_gate_fail_close_range():
    df = pd.DataFrame({
        "close": [100, 105, 102, 104, 103] * 3,
        "high":  [106, 107, 106, 107, 106] * 3,
        "low":   [99, 100, 99, 100, 99] * 3,
    })

    assert price_tightness_gate(df, lookback=10) is False


def test_price_tightness_gate_fail_wide_candle():
    df = pd.DataFrame({
        "close": [100] * 15,
        "high":  [104] * 14 + [110],  # wide range day
        "low":   [98] * 15,
    })

    assert price_tightness_gate(df, lookback=15) is False
