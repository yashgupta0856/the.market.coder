import pandas as pd
from scanners.vcp_scanner import volatility_contraction_gate


def test_volatility_gate_pass():
    """ATR contracting over 10 days and within 8% of price."""
    atr_values = [3.0, 2.8, 2.6, 2.4, 2.2, 2.0, 1.8, 1.6, 1.4, 1.2]
    df = pd.DataFrame({
        "atr_14": atr_values,
        "close": [100.0] * 10,
    })

    assert volatility_contraction_gate(df) is True


def test_volatility_gate_fail_atr_expanding():
    """ATR today >= ATR 10 days ago → fail."""
    atr_values = [1.2, 1.4, 1.6, 1.8, 2.0, 2.2, 2.4, 2.6, 2.8, 3.0]
    df = pd.DataFrame({
        "atr_14": atr_values,
        "close": [100.0] * 10,
    })

    assert volatility_contraction_gate(df) is False


def test_volatility_gate_fail_atr_ratio():
    """ATR/price > 8% → fail."""
    atr_values = [15.0, 14.0, 13.0, 12.0, 11.0, 10.0, 9.5, 9.2, 9.0, 8.5]
    df = pd.DataFrame({
        "atr_14": atr_values,
        "close": [100.0] * 10,
    })

    assert volatility_contraction_gate(df) is False
