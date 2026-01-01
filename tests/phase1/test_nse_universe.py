def test_nse_universe_download():
    from data.raw.nse_universe import download_equity_universe

    df = download_equity_universe()

    assert df is not None
    assert not df.empty
    assert "SYMBOL" in df.columns


def test_universe_cleaner():
    import pandas as pd
    from data.processed.universe_cleaner import clean_equity_universe

    raw = pd.DataFrame({
        "SYMBOL": ["ABC", "XYZ"],
        "SERIES": ["EQ", "BE"],
        "DATE OF LISTING": ["01-Jan-2020", "01-Jan-2020"]
    })

    clean = clean_equity_universe(raw)

    assert len(clean) == 1
    assert clean.iloc[0]["SYMBOL"] == "ABC"
