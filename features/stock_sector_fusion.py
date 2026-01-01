import pandas as pd


def fuse_stock_with_sector(
    stock_indicators_path,
    vcp_path,
    sector_regime_path,
    stock_sector_map_path,
):
    # Load data
    stocks = pd.read_csv(stock_indicators_path, parse_dates=["date"])
    vcp = pd.read_csv(vcp_path)
    sector_regime = pd.read_csv(sector_regime_path)
    mapping = pd.read_csv(stock_sector_map_path)

    # Use latest date only (same philosophy as VCP)
    latest_date = stocks["date"].max()
    stocks_latest = stocks[stocks["date"] == latest_date]

    # Merge VCP flag
    stocks_latest = stocks_latest.merge(
        vcp, on="symbol", how="left"
    )

    stocks_latest["vcp_candidate"] = (
        stocks_latest["vcp_candidate"].fillna(False)
    )

    # Merge stock → sector mapping
    stocks_latest = stocks_latest.merge(
        mapping, on="symbol", how="left"
    )

    # Merge sector regime
    stocks_latest = stocks_latest.merge(
        sector_regime,
        on="sector_index",
        how="left"
    )

    return stocks_latest
