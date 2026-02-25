"""
Gold Price Scraper for Airflow
"""
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import os
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def scrape_gold_prices(days=365, output_dir='data'):
    try:
        logger.info(f"Starting gold price scraping for {days} days...")

        end_date = datetime.now()
        start_date = end_date - timedelta(days=days)

        os.makedirs(output_dir, exist_ok=True)

        RAW_FILE = os.path.join(output_dir, "gold_prices_raw.csv")

        gold = yf.Ticker("GC=F")
        df_new = gold.history(start=start_date, end=end_date)

        if df_new.empty:
            raise ValueError("No data retrieved from yfinance")

        df_new = df_new.reset_index()
        df_new = df_new[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df_new['Date'] = pd.to_datetime(df_new['Date']).dt.date

        if os.path.exists(RAW_FILE):
            logger.info("Raw file exists. Updating existing file...")
            df_old = pd.read_csv(RAW_FILE)
            df_old['Date'] = pd.to_datetime(df_old['Date']).dt.date

            df = pd.concat([df_old, df_new], ignore_index=True)
            df = df.drop_duplicates(subset=['Date'], keep='last')
        else:
            logger.info("Raw file does not exist. Creating new file...")
            df = df_new

        df = df.sort_values('Date').reset_index(drop=True)
        df.to_csv(RAW_FILE, index=False)

        logger.info(f"✅ Raw data updated: {RAW_FILE}")
        return RAW_FILE
    
    except Exception as e:
        logger.error(f"❌ Error during scraping: {str(e)}")
        raise

if __name__ == "__main__":
    scrape_gold_prices()