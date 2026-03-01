import time
from datetime import datetime, timedelta
from modules.db_loader import postgres
from modules.url_parser import yf_pipeline

#Converts total seconds into a readable string (e.g., 5m 30s).
def format_duration(seconds):
    minutes, sec = divmod(int(seconds), 60)
    if minutes > 0:
        return f"{minutes}m {sec}s"
    return f"{sec}s"

if __name__ == '__main__':
    # Fetching data from yfinance
    start_time = time.time()

    postgres.create_ohlcv_table()

    df_companies = postgres.get_company_static()
    ticker_list = [symbol.strip() for symbol in df_companies['symbol'].tolist()]
    
    last_update = postgres.get_latest_date()

    if last_update:
        start = (last_update - timedelta(days=20)).strftime('%Y-%m-%d')
        yf_pipeline.fetch_ohlcv_data(ticker_list, start_date=start)
    else:
        yf_pipeline.fetch_ohlcv_data(ticker_list)

    end_time = time.time()
    duration = end_time - start_time
    print("-" * 30)
    print(f"Pipeline execution finished.")
    print(f"Total duration: {format_duration(duration)}")
    print("-" * 30)
