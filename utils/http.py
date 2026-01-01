import requests

def safe_get(url , headers = None, timeout = 30):

    """

    raise_for_status() -> Automatically raise an exception if the HTTP request returned an unsuccessful status code (anything outside the range 200 - 299).

    """
    response = requests.get(url , headers=headers, timeout=timeout)
    response.raise_for_status()

    return response