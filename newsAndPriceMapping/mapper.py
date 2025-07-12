import pandas as pd
import requests
import time
from datetime import datetime, timedelta
import os

class NewsToFuturePriceMapper:
    def __init__(self, news_csv_path, verbose=False, shuffle=True, rows_limit = None):
        self.news_csv_path = news_csv_path
        self.base_url = "https://api.binance.com/api/v3"
        self.session = requests.Session()
        self.verbose = verbose
        self.shuffle = shuffle
        self.rows_limit = rows_limit
        
        self.time_intervals = {
            '1m': 1,
            '5m': 5,
            '30m': 30,
            '1h': 60,
            # '1d': 1440
        }
        
    def load_news_data(self):
        """
        Upload news data from CSV file
        """
        try:
            df = pd.read_csv(self.news_csv_path)

            assert 'date_time' in df.columns, "CSV file must contain 'date_time' column"
            assert 'title' in df.columns, "CSV file must contain 'title' column"

            df['date_time'] = pd.to_datetime(df['date_time'])

            if self.shuffle:
                df = df.drop_duplicates('date_time') # Remove duplicates by date
                df = df.sample(frac=1).reset_index(drop=True)
            
            if self.rows_limit:
                df = df.iloc[:self.rows_limit]

            if self.verbose:
                print(f"Loaded {len(df)} news articles")
                print(f'Shuffle is {self.shuffle}')
                print(f'Rows limit is {self.rows_limit != None}')
            return df
        except Exception as e:
            print(f"Failed to load: {e}")
            return None
    
    def get_kline_at_time(self, symbol, target_time, time_delta=1, interval='1m', limit=1):
        """
        Extracts price at specific time for a given symbol and interval
        
        Args:
            symbol: Ticker 
            target_time: target datetime
            time_delta: time delta to end_time in minutes
            interval: kline interval
            limit: number of klines
        """
        
        start_time = int(target_time.timestamp() * 1000)
        end_time = int((target_time + timedelta(minutes=time_delta)).timestamp() * 1000)
        
        url = f"{self.base_url}/klines"
        params = {
            'symbol': symbol,
            'interval': interval,
            'startTime': start_time,
            'endTime': end_time,
            'limit': limit
        }
        
        try:
            response = self.session.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            if data:
                return float(data[0][4])  # close price
            else:
                return None
                
        except requests.exceptions.RequestException as e:
            if self.verbose:
                print(f"Error for {target_time}: {e}")
            return None
    
    def get_future_prices(self, news_time, symbol='BTCUSDT'):
        """
        Extracts future prices for given time and symbol
        
        Args:
            news_time: datetime
            symbol: Ticker
        """
        future_prices = {}
        
        for interval_name, minutes in self.time_intervals.items():
            future_time = news_time + timedelta(minutes=minutes)
            
            if future_time > datetime.now():
                future_prices[f'price_after_{interval_name}'] = None
                continue
            
            price = self.get_kline_at_time(symbol, future_time)
            future_prices[f'price_after_{interval_name}'] = price
            
            if self.verbose and price:
                print(f"Price after {interval_name} for {news_time}: {price}")
        
        return future_prices
    
    def calculate_price_changes(self, current_price, future_price):
        """
        Calculates percentage diff for price
        """
        if future_price and current_price:
            change_percent = round(((future_price - current_price) / current_price) * 100, 2)
        else:
            change_percent = None
        
        return change_percent
    
    def get_current_price(self, news_time, symbol='BTCUSDT'):
        """
        Extracts current price for given symbol
        """
        return self.get_kline_at_time(symbol, news_time)
    
    def process_news_data(self, symbol='BTCUSDT'):
        """
        Main function for processing news data
        """
        news_df = self.load_news_data()
        if news_df is None:
            return None
        
        result_df = news_df.copy()
        
        for interval_name in self.time_intervals.keys():
            result_df[f'price_after_{interval_name}'] = None
            result_df[f'price_change_{interval_name}_percent'] = None
        
        result_df['current_price'] = None
        
        for index, row in result_df.iterrows():
            news_time = row['date_time']
            
            if self.verbose:
                print(f"\n Processing {index + 1}/{len(result_df)}: {news_time}")
                print(f"Title: {row['title'][:50]}...")
            
            current_price = self.get_current_price(news_time, symbol) # Extract current price
            result_df.at[index, 'current_price'] = current_price
            
            future_prices = self.get_future_prices(news_time, symbol)
            
            for col_name, price in future_prices.items():
                result_df.at[index, col_name] = price
            
            for interval_name in self.time_intervals.keys():
                future_price = result_df.at[index, f'price_after_{interval_name}']

                result_df.at[index, f'price_change_{interval_name}_percent'] = self.calculate_price_changes(current_price, future_price)
        
        return result_df
    
    def save_results(self, df, filename='news_with_future_prices.csv'):
        """
        Saves results to CSV
        """
        if df is not None:
            os.makedirs('results', exist_ok=True)
            filepath = os.path.join('results', filename)
            
            df.to_csv(filepath, index=False)
            
            if self.verbose:
                print(f"\nSaved to {filepath}")
                print(f"News processed: {len(df)}")
                
                non_null_counts = {}
                for interval_name in self.time_intervals.keys():
                    col_name = f'price_after_{interval_name}'
                    non_null_count = df[col_name].notna().sum()
                    non_null_counts[interval_name] = non_null_count
                
                print("\nStats:")
                for interval, count in non_null_counts.items():
                    print(f"{interval}: {count}/{len(df)} ({count/len(df)*100:.1f}%)")
        else:
            print("No data to save")
    
    def run(self, symbol='BTCUSDT', output_filename='news_with_future_prices.csv'):
        """
        Starts the whole process
        """
        print(f"Started for {symbol} at {datetime.now()}")
        
        result_df = self.process_news_data(symbol)
        
        self.save_results(result_df, output_filename)
        
        return result_df

if __name__ == "__main__":
    # Path to your CSV file with required columns
    news_file_path = "news.csv" # Required columns: date_time, title
    
    mapper = NewsToFuturePriceMapper(news_file_path, verbose=True, rows_limit=100)
    
    result = mapper.run(symbol='BTCUSDT', output_filename='btc_news_with_future_prices.csv')
    
    if result is not None:
        print("\nData example:")
        print(result[['date_time', 'title', 'current_price', 'price_after_1m', 'price_change_1m_percent']].head())