import pandas as pd
from scanners.vcp_scanner import volume_dryup_gate


def test_volume_gate_pass():
    """Volume dries up in recent window — should pass."""
    df = pd.DataFrame({
        "close": [100 + i * 0.1 for i in range(50)],
        "volume": [1000] * 35 + [700] * 15,
    })

    assert volume_dryup_gate(df) is True


def test_volume_gate_fail_high_recent_volume():
    """Recent volume > 1.5× prior volume — should fail."""
    df = pd.DataFrame({
        "close": [100 + i * 0.1 for i in range(50)],
        "volume": [400] * 35 + [1200] * 15,  # 1200/400 = 3× expansion
    })

    assert volume_dryup_gate(df) is False


def test_volume_gate_fail_distribution_day():
    """Heavy distribution days (>40% of recent window) — should fail."""
    # 8 out of 15 recent days are down on high volume = 53% distribution
    closes = [100 + i * 0.1 for i in range(35)]
    closes += [105, 104, 105, 103, 105, 102, 105, 101, 105, 100, 105, 99, 105, 98, 105]

    avg_vol = 1000
    volumes = [avg_vol] * 35
    # Alternate: down days get 2× average volume (exceed 1.5× threshold)
    volumes += [2000, 2000, 2000, 2000, 2000, 2000, 2000, 2000,
                2000, 2000, 2000, 2000, 2000, 2000, 2000]

    df = pd.DataFrame({
        "close": closes,
        "volume": volumes,
    })

    assert volume_dryup_gate(df) is False
