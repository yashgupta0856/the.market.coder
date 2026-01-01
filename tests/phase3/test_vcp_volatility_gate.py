import pandas as pd
from scanners.vcp_scanner import volatility_contraction_gate


def test_volatility_gate_pass():
    df = pd.DataFrame({
        "atr_14": [1.2],
        "atr_100": [2.0],
        "range_compression": [0.7],
    })

    assert volatility_contraction_gate(df) is True


def test_volatility_gate_fail_atr_ratio():
    df = pd.DataFrame({
        "atr_14": [2.0],
        "atr_100": [2.1],
        "range_compression": [0.7],
    })

    assert volatility_contraction_gate(df) is False


def test_volatility_gate_fail_range_compression():
    df = pd.DataFrame({
        "atr_14": [1.2],
        "atr_100": [2.0],
        "range_compression": [1.2],
    })

    assert volatility_contraction_gate(df) is False
