import json
from pathlib import Path


SUMMARY_PATH = Path("web/static/vcp_backtest_summary.json")


def get_backtest_summary():
    if not SUMMARY_PATH.exists():
        return None

    with open(SUMMARY_PATH) as f:
        return json.load(f)
