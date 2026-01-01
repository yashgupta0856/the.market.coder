def validate_ohlcv(df):
    if df is None or df.empty:
        return False

    return (
        (df["high"] >= df[["open", "close"]].max(axis=1)).all()
        and (df["low"] <= df[["open", "close"]].min(axis=1)).all()
        and (df["volume"] >= 0).all()
        and (df["open"] > 0).all()
        and (df["close"] > 0).all()
    )
