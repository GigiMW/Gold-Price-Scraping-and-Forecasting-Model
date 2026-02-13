"""
Simple Gold Price Scraper - Fixed Version
Scrapes gold prices using yfinance library
"""

import pandas as pd
from datetime import datetime, timedelta

def scrape_gold_prices(days=365):
    """
    Scrape gold prices using yfinance
    
    Args:
        days (int): Number of days to scrape (default: 365)
    
    Returns:
        pandas.DataFrame: Gold price data
    """
    try:
        import yfinance as yf
    except ImportError:
        print("Installing yfinance...")
        import subprocess
        subprocess.check_call(['pip', 'install', 'yfinance'])
        import yfinance as yf
    
    print(f"Scraping {days} days of gold prices...")
    
    # Calculate date range
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    try:
        # Download gold futures data (GC=F)
        gold = yf.Ticker("GC=F")
        df = gold.history(start=start_date, end=end_date)
        
        # Reset index to make Date a column
        df = df.reset_index()
        
        # Rename columns
        df = df.rename(columns={
            'Date': 'Date',
            'Open': 'Open',
            'High': 'High', 
            'Low': 'Low',
            'Close': 'Close',
            'Volume': 'Volume'
        })
        
        # Keep only needed columns
        df = df[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        
        # Format date
        df['Date'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
        
        print(f"✅ Successfully scraped {len(df)} records")
        return df
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return pd.DataFrame()


if __name__ == "__main__":
    # Scrape gold prices
    df = scrape_gold_prices(days=365)
    
    if not df.empty:
        # Save to CSV
        filename = f"gold_prices_{datetime.now().strftime('%Y%m%d')}.csv"
        df.to_csv(filename, index=False)
        print(f"\n✅ Data saved to: {filename}")
        
        # Show first few rows
        print("\nFirst 10 rows:")
        print(df.head(10).to_string(index=False))
        
        # Show basic stats
        print("\nBasic Statistics:")
        print(df[['Open', 'High', 'Low', 'Close']].describe())
        
        print(f"\nPrice Range:")
        print(f"Lowest:  ${df['Low'].min():,.2f}")
        print(f"Highest: ${df['High'].max():,.2f}")
        print(f"Current: ${df['Close'].iloc[-1]:,.2f}")
    else:
        print("\n❌ Failed to scrape data")
