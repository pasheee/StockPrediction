from ParserModule import ParserModule
import requests
import pandas as pd

class NewsParserAPI(ParserModule):
    def __init__(self, data=None):
        super().__init__()


    def load_data(self, **kwargs):
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

        tickers = kwargs.get('tickers', 'CRYPTO:BTC')
        time_from = kwargs.get('time_from', '20230101T0000')
        time_to = kwargs.get('time_to', '20230131T2359')
        limit = kwargs.get('limit', '1000')
        sort_type = kwargs.get('sort_type', 'LATEST')
        API_KEY = kwargs.get('API_KEY', 'N7JPSQ1APAY1A586')


        url = 'https://www.alphavantage.co/query?function=NEWS_SENTIMENT' \
        + '&tickers=' + tickers \
        + '&limit=' + limit \
        + '&time_from=' + time_from \
        + '&time_to=' + time_to \
        + '&sort=' + sort_type \
        + '&apikey=' + API_KEY

        r = requests.get(url)
        data = r.json()
        self.set_data(data)

        return data
    

    @staticmethod
    def _parsing_news_dict(data: dict) -> list:
        """
        Parses news data from a dictionary format into a list of tuples.

        Args:
            data (dict): A dictionary containing news items with 'feed' and 'items' keys.

        Returns:
            list: A list of tuples, each containing the time and parsed news string.
        """
        num_items = data['items']
        news_data = data['feed']
        
        parsed_news = []
        for item in news_data:
            parsed_str = 'Title: ' + item['title'] + '; Summary: ' + item['summary']
            time = int(item['time_published'].replace('T', ''))
            parsed_news.append((time, parsed_str))
        
        return parsed_news


    def parse_data(self, time_from: str, time_to: str, **kwargs):
        """
        Fetches, parses, and stores news data in the database.

        Args:
            time_from (str): Start date/time for news data in ISO format (e.g., '20230101T0000').
            time_to (str): End date/time for news data in ISO format (e.g., '20230131T2359').

        Keyword Args:
            limit (str, optional): Maximum number of news items to retrieve. Defaults to '1000'.
            tickers (str, optional): Ticker symbols to fetch news for (e.g., 'CRYPTO:BTC'). Defaults to 'CRYPTO:BTC'.
            API_KEY (str, optional): API key for Alpha Vantage. Defaults to 'N7JPSQ1APAY1A586'.
        """

        params = {
            'limit': kwargs.get('limit', '1000'),
            'tickers': kwargs.get('tickers', 'CRYPTO:BTC'),
            'API_KEY': kwargs.get('API_KEY', 'N7JPSQ1APAY1A586'),
        }

        parsed_news = self._parsing_news_dict(self.get_data())
        self.set_data(parsed_news)

        return parsed_news