# funcs here
from functions.newsAPI import get_api_data_news
from functions.parsing_news_dict import parsing_news_dict
from functions.create_writeto_database import create_writeto_database

def parse_and_store_news(time_from: str, time_to: str, **kwargs) -> None:
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
        'API_KEY': kwargs.get('API_KEY', 'N7JPSQ1APAY1A586')
    }
    data = get_api_data_news(
        time_from,
        time_to,
        params['limit'],
        params['tickers'],
        params['API_KEY']
    )
    
    parsed_news = parsing_news_dict(data)
    create_writeto_database(parsed_news)