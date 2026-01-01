import pandas as pd
from scanners.vcp_scanner import volume_dryup_gate


def test_volume_gate_pass():
    df = pd.DataFrame({
        "close": [100 + i*0.1 for i in range(50)],
        "volume": [1000]*35 + [700]*15,  # volume dries up
    })

    assert volume_dryup_gate(df) is True


def test_volume_gate_fail_high_recent_volume():
    df = pd.DataFrame({
        "close": [100 + i*0.1 for i in range(50)],
        "volume": [800]*35 + [1200]*15,
    })

    assert volume_dryup_gate(df) is False


def test_volume_gate_fail_distribution_day():
    volumes = [1000]*35 + [700]*14 + [2000]
    closes = [100 + i*0.1 for i in range(49)] + [95]  # down day

    df = pd.DataFrame({
        "close": closes,
        "volume": volumes,
    })

    assert volume_dryup_gate(df) is False
