import pandas as pd
from io import StringIO
from utils.http import safe_get
from configs.data_sources import NSE_EQUITY_URL


def download_equity_universe():

    headers = {
        "User-Agent":"Mozilla/5.0",
        "Accept":"text/csv"
    }

    response = safe_get(NSE_EQUITY_URL, headers=headers)
    csv_buffer = StringIO(response.text)

    return pd.read_csv(csv_buffer)
