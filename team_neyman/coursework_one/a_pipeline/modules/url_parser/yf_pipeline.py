import numpy as np
import pandas as pd
import yfinance as yf
import time
from datetime import datetime, timedelta
from modules.db_loader import postgres
from modules.factors import calculate_factors

def fetch_ohlcv_data(ticker_list: list, start_date=None, end_date=None):
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date is None:
        five_years_ago = datetime.now() - timedelta(days=5*365)
        start_date = five_years_ago.strftime('%Y-%m-%d')
    print(f"Starting pipeline: Fetching from {start_date} to {end_date}")

    batch_size = 50 

    for i in range(0, len(ticker_list), batch_size):
        batch = ticker_list[i : i + batch_size]
        
        try:
            raw_data = yf.download(batch, start=start_date, end=end_date, interval="1d", auto_adjust=True)
            if raw_data.empty:
                print(f"Batch {batch} returned no data at all.")
                continue

            clean_data = raw_data.dropna(axis=1, how='all')
            if clean_data.empty:
                continue

            df_stacked = clean_data.stack(level=1, future_stack=True).reset_index()
            df_stacked = df_stacked.rename(columns={
                'Date': 'price_date',
                'Ticker': 'symbol',
                'Open': 'open_price',
                'High': 'high_price',
                'Low': 'low_price',
                'Close': 'close_price',
                'Volume': 'volume'
            })
            df_stacked['symbol'] = df_stacked['symbol'].str.strip()
            df_stacked['price_date'] = pd.to_datetime(df_stacked['price_date']).dt.date
            price_cols = ['open_price', 'high_price', 'low_price', 'close_price']
            for col in price_cols:
                df_stacked[col] = pd.to_numeric(df_stacked[col], errors='coerce').round(4)
            df_stacked['volume'] = df_stacked['volume'].replace([np.inf, -np.inf], 0).fillna(0).astype('int64')
            df_stacked = df_stacked.dropna(subset=['symbol', 'price_date', 'close_price'])

            postgres.update_ohlcv_data(df_stacked)
            print(f"Batch {i//batch_size + 1} uploaded successfully.")
            del df_stacked
            
            time.sleep(2) 
            
        except Exception as e:
            print(f"Error downloading batch starting with {batch[0]}: {e}")

def calculate_liquidity_data(ticker_list: list):
    data = postgres.get_ohlcv_data(ticker_list)
    data['return'] = calculate_factors.calculate_return(data)
    data['dollar_volume'] = calculate_factors.calculate_dollar_volume(data)
    data['adv_20d'] = calculate_factors.calculate_avg_volume(data, days=20)
    data['adv_60d'] = calculate_factors.calculate_avg_volume(data, days=60)
    data['mdv_20d'] = calculate_factors.calculate_median_volume(data, days=20)
    data['mdv_60d'] = calculate_factors.calculate_median_volume(data, days=60)
    data['addv_20d'] = calculate_factors.calculate_avg_dollar_volume(data, days=20)
    data['addv_60d'] = calculate_factors.calculate_avg_dollar_volume(data, days=60)
    data['mddv_20d'] = calculate_factors.calculate_median_dollar_volume(data, days=20)
    data['mddv_60d'] = calculate_factors.calculate_median_dollar_volume(data, days=60)
    data['amihud_illiquidity_20d'] = calculate_factors.calculate_amihud(data, days=20)
    data['amihud_illiquidity_60d'] = calculate_factors.calculate_amihud(data, days=60)
    postgres.update_liquidity_data(data)

def calculate_trend_data(ticker_list: list):
    data = postgres.get_ohlcv_data(ticker_list)
    data['ma200'] = calculate_factors.calculate_ema(data, days=200)
    data['ma150'] = calculate_factors.calculate_ema(data, days=150)
    data['ma100'] = calculate_factors.calculate_ema(data, days=100)
    data['adx14'] = calculate_factors.calculate_adx(data, days=14)
    data['donchian_high_55'] = calculate_factors.calculate_donchian_high(data, days=55)
    data['donchian_high_120'] = calculate_factors.calculate_donchian_high(data, days=120)
    data['price_to_52w_high'] = calculate_factors.calculate_price_to_weeks_high(data, days=252)
    postgres.update_trend_data(data)

def calculate_momentum_data(ticker_list: list):
    data = postgres.get_ohlcv_data(ticker_list)

def calculate_risk_data(ticker_list: list):
    data = postgres.get_ohlcv_data(ticker_list)

def calculate_mean_reversion_data(ticker_list: list):
    data = postgres.get_ohlcv_data(ticker_list)
