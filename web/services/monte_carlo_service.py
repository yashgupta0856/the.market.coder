import json
from pathlib import Path

MONTE_CARLO_PATH = "data/processed/monte_carlo.json"


def get_monte_carlo_for_symbol(symbol: str):
    if not Path(MONTE_CARLO_PATH).exists():
        return None

    with open(MONTE_CARLO_PATH) as f:
        data = json.load(f)

    return data.get(symbol)