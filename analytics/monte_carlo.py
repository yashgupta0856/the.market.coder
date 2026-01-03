import numpy as np
import pandas as pd


def run_monte_carlo(
    price_series: pd.Series,
    days: int = 5,
    simulations: int = 1000,
):
    returns = np.log(price_series / price_series.shift(1)).dropna()

    mu = returns.mean()
    sigma = returns.std()

    last_price = price_series.iloc[-1]

    paths = []
    final_prices = []

    for _ in range(simulations):
        random_returns = np.random.normal(mu, sigma, days)
        price_path = last_price * np.exp(np.cumsum(random_returns))
        paths.append(price_path.tolist())
        final_prices.append(price_path[-1])

    final_prices = np.array(final_prices)
    pct_returns = (final_prices / last_price) - 1

    metrics = {
        "expected_return_pct": round(pct_returns.mean() * 100, 2),
        "probability_of_loss_pct": round((pct_returns < 0).mean() * 100, 2),
        "probability_of_5pct_gain_pct": round((pct_returns > 0.05).mean() * 100, 2),
        "worst_5pct_outcome_pct": round(np.percentile(pct_returns, 5) * 100, 2),
    }

    return {
        "metrics": metrics,
        "paths": paths
    }