import requests
<<<<<<< HEAD
def get_api_data_news(time_from, time_to, limit = '1000', tickers = "CRYPTO:BTC", API_KEY="N7JPSQ1APAY1A586"):
=======

def get_api_data_news(time_from: str, time_to: str, limit: str = '1000', tickers: str = "CRYPTO:BTC", API_KEY: str = "N7JPSQ1APAY1A586") -> dict:
>>>>>>> 626604d (fd)
    """
    Fetches news sentiment data from the Alpha Vantage API for specified tickers and time range.
    Args:
        time_from (str): The start date/time for the news data in ISO format (e.g., '20230101T0000').
        time_to (str): The end date/time for the news data in ISO format (e.g., '20230131T2359').
        limit (str, optional): The maximum number of news items to retrieve. Defaults to '1000'.
        tickers (str, optional): The ticker symbols to fetch news for (e.g., 'CRYPTO:BTC'). Defaults to 'CRYPTO:BTC'.
        API_KEY (str, optional): The API key for Alpha Vantage. Defaults to 'N7JPSQ1APAY1A586'.
    Returns:
        dict: The JSON response from the API as a Python dictionary.
    Raises:
        requests.exceptions.RequestException: If the HTTP request fails.
        ValueError: If the response cannot be decoded as JSON.
    """

    url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT' \
    + '&tickers=' + tickers \
    + '&limit=' + limit \
    + '&time_from=' + time_from \
    + '&time_to=' + time_to \
    + '&apikey=' + API_KEY

    r = requests.get(url)
    data = r.json()
    return dict(data)