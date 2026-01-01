import pandas as pd

def clean_equity_universe(df:pd.DataFrame) -> pd.DataFrame:

    df.columns = [c.strip() for c in df.columns]

    df = df[df['SERIES'] == "EQ"].copy()
    df["SYMBOL"] = df["SYMBOL"].str.strip()

    df["DATE OF LISTING"] = pd.to_datetime(
        df["DATE OF LISTING"],
        format="%d-%b-%Y",
        errors="coerce"
    )

    df = df.dropna(subset=["SYMBOL","DATE OF LISTING"])
    df = df.sort_values("SYMBOL").reset_index(drop=True)

    return df