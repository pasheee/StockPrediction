def parsing_news_dict(data: dict) -> list:
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