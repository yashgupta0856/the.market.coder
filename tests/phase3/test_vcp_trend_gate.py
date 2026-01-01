import pandas as pd
from scanners.vcp_scanner import trend_gate


def test_trend_gate_pass():
    df = pd.DataFrame({
        "close": [100],
        "sma_50": [90],
        "sma_200": [80],
        "reg_slope_100": [1.5],
    })

    assert trend_gate(df) is True


def test_trend_gate_fail_price_below_ma():
    df = pd.DataFrame({
        "close": [85],
        "sma_50": [90],
        "sma_200": [80],
        "reg_slope_100": [1.2],
    })

    assert trend_gate(df) is False
