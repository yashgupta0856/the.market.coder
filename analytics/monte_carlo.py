import numpy as np
import pandas as pd


def run_monte_carlo(
    price_series: pd.Series,
    days: int = 5,
    simulations: int = 1000,
):
    """
    Fully vectorised Monte Carlo price simulation.

    Generates *simulations* random-walk paths of length *days* using
    log-normal returns calibrated from the input price history.
    """
    returns = np.log(price_series / price_series.shift(1)).dropna()

    mu = returns.mean()
    sigma = returns.std()

    last_price = float(price_series.iloc[-1])

    # Vectorised: (simulations × days) matrix of random returns
    random_returns = np.random.normal(mu, sigma, (simulations, days))
    price_paths = last_price * np.exp(np.cumsum(random_returns, axis=1))

    final_prices = price_paths[:, -1]
    pct_returns = (final_prices / last_price) - 1

    metrics = {
        "expected_return_pct": round(float(pct_returns.mean() * 100), 2),
        "probability_of_loss_pct": round(float((pct_returns < 0).mean() * 100), 2),
        "probability_of_5pct_gain_pct": round(float((pct_returns > 0.05).mean() * 100), 2),
        "worst_5pct_outcome_pct": round(float(np.percentile(pct_returns, 5) * 100), 2),
    }

    return {
        "metrics": metrics,
        "paths": price_paths.tolist(),
    }