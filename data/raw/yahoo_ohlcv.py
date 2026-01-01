import yfinance as yf
from configs.data_sources import YAHOO_SUFFIX

def fetch_ohlcv(symbol: str, start_date=None):
    ticker = yf.Ticker(f"{symbol}{YAHOO_SUFFIX}")

    df = ticker.history(
        start=start_date,
        interval="1d",
        auto_adjust=False
    )

    if df.empty:
        return None

    df = df.reset_index()

    df.rename(
        columns={
            "Date": "date",
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        },
        inplace=True
    )

    df["symbol"] = symbol
    return df
