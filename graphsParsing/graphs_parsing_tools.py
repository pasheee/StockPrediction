import requests
import pandas as pd
import numpy as np

import time
from datetime import datetime, timedelta
from finta import TA
import json
import os

class BinanceDataCollector:
    def __init__(self):
        self.base_url = "https://api.binance.com/api/v3"
        self.verbose = False
        self.session = requests.Session()
        
    def get_klines(self, symbol, interval, start_time, end_time, limit=1000):
        """
        Получает данные свечей с Binance API
        
        Args:
            symbol: торговая пара (например, 'BTCUSDT')
            interval: интервал ('1m', '5m', '1h', '1d')
            start_time: начальное время (timestamp в миллисекундах)
            end_time: конечное время (timestamp в миллисекундах)
            limit: количество свечей за запрос (макс 1000)
        """
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
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка при запросе данных: {e}")
            return None
    
    def collect_historical_data(self, symbol, interval, days_back=730):
        """
        Собирает исторические данные за указанный период
        
        Args:
            symbol: торговая пара
            interval: интервал
            days_back: количество дней назад (по умолчанию 730 = 2 года)
        """
        if self.verbose:
            print(f"Начинаем сбор данных для {symbol} с интервалом {interval}")
        
        end_time = datetime.now()
        start_time = end_time - timedelta(days=days_back)
        
        start_timestamp = int(start_time.timestamp() * 1000)
        end_timestamp = int(end_time.timestamp() * 1000)
        
        all_data = []
        current_start = start_timestamp
        
        interval_ms = self._get_interval_ms(interval)
        
        while current_start < end_timestamp:
            current_end = min(current_start + (1000 * interval_ms), end_timestamp)
            
            if self.verbose:
                print(f"Загружаем данные с {datetime.fromtimestamp(current_start/1000)} по {datetime.fromtimestamp(current_end/1000)}")
            
            klines = self.get_klines(symbol, interval, current_start, current_end)
            
            if klines:
                all_data.extend(klines)
                if self.verbose:
                    print(f"Получено {len(klines)} свечей")
            else:
                print("Ошибка при получении данных")
                break
            
            current_start = current_end + interval_ms
            
            time.sleep(0.1)
        
        return all_data
    
    def _get_interval_ms(self, interval):
        """Конвертирует интервал в миллисекунды"""
        intervals = {
            '1m': 60 * 1000,
            '3m': 3 * 60 * 1000,
            '5m': 5 * 60 * 1000,
            '15m': 15 * 60 * 1000,
            '30m': 30 * 60 * 1000,
            '1h': 60 * 60 * 1000,
            '2h': 2 * 60 * 60 * 1000,
            '4h': 4 * 60 * 60 * 1000,
            '6h': 6 * 60 * 60 * 1000,
            '8h': 8 * 60 * 60 * 1000,
            '12h': 12 * 60 * 60 * 1000,
            '1d': 24 * 60 * 60 * 1000,
            '3d': 3 * 24 * 60 * 60 * 1000,
            '1w': 7 * 24 * 60 * 60 * 1000,
            '1M': 30 * 24 * 60 * 60 * 1000
        }
        return intervals.get(interval, 60 * 1000)
    
    def process_data_to_dataframe(self, raw_data):
        """
        Преобразует сырые данные в pandas DataFrame
        """
        if not raw_data:
            return pd.DataFrame()
        
        df = pd.DataFrame(raw_data, columns=[
            'open_time', 'open', 'high', 'low', 'close', 'volume',
            'close_time', 'quote_asset_volume', 'number_of_trades',
            'taker_buy_base_asset_volume', 'taker_buy_quote_asset_volume', 'ignore'
        ])
        
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df['close_time'] = pd.to_datetime(df['close_time'], unit='ms')
        
        price_volume_columns = ['open', 'high', 'low', 'close', 'volume', 
                               'quote_asset_volume', 'taker_buy_base_asset_volume', 
                               'taker_buy_quote_asset_volume']
        
        for col in price_volume_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        
        df['number_of_trades'] = pd.to_numeric(df['number_of_trades'], errors='coerce')
        
        df = df.drop('ignore', axis=1)
        
        df = df.sort_values('open_time').reset_index(drop=True)
        
        self.df = df

        return df
    
    def save_data(self, df, filename, dirname='train_graph_dataset'):
        """Сохраняет данные в CSV файл"""
        os.makedirs(dirname, exist_ok=True)
        filepath = os.path.join(dirname, filename)

        df.to_csv(filepath, index=False)
        self.df = df
        if self.verbose:
            print(f"Данные сохранены в {filepath}")
    
    def load_data(self, filename):
        """Загружает данные из CSV файла"""
        filepath = f'data/{filename}'
        if os.path.exists(filepath):
            df = pd.read_csv(filepath)
            df['open_time'] = pd.to_datetime(df['open_time'])
            df['close_time'] = pd.to_datetime(df['close_time'])
            self.df = df 
            return df
        else:
            print(f"Файл {filepath} не найден")
            return None

    def add_all_indicators_finta(self):
        """
        Добавляет все популярные индикаторы используя finta lib
        Возвращает DataFrame с добавленными индикаторами
        """
        result_df = self.df.copy()
        
        # Скользящие средние
        result_df['SMA_20'] = TA.SMA(result_df, 20) # За 20 последний периодов
        result_df['EMA_12'] = TA.EMA(result_df, 12) # За 12 последний периодов
        
        # Momentum индикаторы
        result_df['RSI'] = TA.RSI(result_df)
        result_df['MACD'] = TA.MACD(result_df)['MACD']
        result_df['MACD_signal'] = TA.MACD(result_df)['SIGNAL']
        result_df['Stoch_K'] = TA.STOCH(result_df)
        result_df['Williams_R'] = TA.WILLIAMS(result_df)
        
        # Volatility индикаторы
        result_df['ATR'] = TA.ATR(result_df)
        bb = TA.BBANDS(result_df)
        result_df['BB_upper'] = bb['BB_UPPER']
        result_df['BB_middle'] = bb['BB_MIDDLE']
        result_df['BB_lower'] = bb['BB_LOWER']
        
        # Volume индикаторы
        result_df['OBV'] = TA.OBV(result_df)
        
        return result_df
    
    def fix_data(self):
        """ Фиксит большинство NaN значений в данных после добавления индикаторов"""

        if self.df is not None:
            data = self.df.copy()

            if 'OBV' in data.columns:
                ids = np.arange(data.shape[0])[data['OBV'].isna() == True]
                prev_ids = ids - 1
                prev_ids[0] = 0
                next_ids = ids + 1

                avg = (data['OBV'].iloc[prev_ids].values + data['OBV'].iloc[next_ids].values) / 2
                data.loc[ids, 'OBV'] = avg
            
            data.bfill(inplace=True)
            na_mx = max(data.isna().sum())

            final_data = data.iloc[na_mx + 1:].bfill() # Некоторые метрики начинаются с NaN поэтому тупо кат делаю <- костыль ^_^

            self.save_data(final_data, 'TA_data_filled_NaN.csv')
            if self.verbose:
                print(" Fixed и сохранены в файл TA_data_filled_NaN.csv")

            return final_data


def main():
    collector = BinanceDataCollector()
    
    symbol = 'BTCUSDT'  
    interval = '1m'     
    days_back = 365     # Days back
    
    print("Начинаем сбор данных...")
    raw_data = collector.collect_historical_data(symbol, interval, days_back)
    
    if raw_data:
        print(f"Собрано {len(raw_data)} свечей")
        
        df = collector.process_data_to_dataframe(raw_data)
        
        print("\nИнформация о данных:")
        print(f"Период: с {df['open_time'].min()} по {df['open_time'].max()}")
        print(f"Количество записей: {len(df)}")
        print(f"Размер данных: {df.shape}")
        
        print("\nПервые 5 записей:")
        print(df.head())
        
        filename = f'{symbol}_{interval}_{days_back}days.csv'
        collector.save_data(df, filename)
        
        print("\nБазовая статистика по ценам:")
        print(df[['open', 'high', 'low', 'close', 'volume']].describe())
    
    else:
        print("Не удалось собрать данные")

def collect_multiple_coins():
    collector = BinanceDataCollector()
    
    symbols = ['BTCUSDT', 'ETHUSDT', 'ADAUSDT', 'DOTUSDT']
    interval = '1m'  
    days_back = 730  
    
    for symbol in symbols:
        print(f"\n{'='*50}")
        print(f"Собираем данные для {symbol}")
        print(f"{'='*50}")
        
        raw_data = collector.collect_historical_data(symbol, interval, days_back)
        
        if raw_data:
            df = collector.process_data_to_dataframe(raw_data)
            filename = f'{symbol}_{interval}_{days_back}days.csv'
            collector.save_data(df, filename)
            
            print(f"Данные для {symbol} сохранены: {len(df)} записей")
        else:
            print(f"Ошибка при сборе данных для {symbol}")
        
        time.sleep(1)

if __name__ == "__main__":
    main()
    # collect_multiple_coins()