"""
Data Cleaning and Preprocessing for Gold Price Data
"""
import os
import pandas as pd
import numpy as np
import logging
from datetime import datetime

# -----------------------------------------------------------------------------
# Logging
# -----------------------------------------------------------------------------
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# -----------------------------------------------------------------------------
# Core Cleaner Class
# -----------------------------------------------------------------------------
class GoldDataCleaner:
    """Handle data cleaning and preprocessing for gold price data"""

    def __init__(self, df: pd.DataFrame):
        """
        Initialize with a pandas DataFrame

        Args:
            df (pd.DataFrame): Raw gold price data
        """
        self.df = df.copy()
        self.cleaning_report = {
            'original_rows': len(df),
            'duplicates_removed': 0,
            'missing_values_handled': 0,
            'outliers_detected': 0,
            'final_rows': 0
        }

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------
    def clean(self) -> pd.DataFrame:
        """
        Main cleaning pipeline

        Returns:
            pd.DataFrame: Cleaned data
        """
        logger.info("Starting data cleaning pipeline...")

        self._convert_data_types()
        self._handle_duplicates()
        self._handle_missing_values()
        self._validate_price_relationships()
        self._detect_outliers()
        self._sort_data()

        self.cleaning_report['final_rows'] = len(self.df)

        self._log_report()
        return self.df

    def get_cleaning_report(self) -> dict:
        """Return the cleaning report"""
        return self.cleaning_report

    # -------------------------------------------------------------------------
    # Internal Steps
    # -------------------------------------------------------------------------
    def _convert_data_types(self):
        logger.info("Converting data types...")

        date_col = 'Date' if 'Date' in self.df.columns else 'date'
        self.df[date_col] = pd.to_datetime(self.df[date_col], errors='coerce')

        price_cols = ['Open', 'High', 'Low', 'Close'] \
            if 'Open' in self.df.columns else ['open', 'high', 'low', 'close']

        for col in price_cols:
            if col in self.df.columns:
                self.df[col] = pd.to_numeric(self.df[col], errors='coerce')

        vol_col = 'Volume' if 'Volume' in self.df.columns else 'volume'
        if vol_col in self.df.columns:
            self.df[vol_col] = (
                pd.to_numeric(self.df[vol_col], errors='coerce')
                .fillna(0)
                .astype(np.int64)
            )

        logger.info("✅ Data types converted")

    def _handle_duplicates(self):
        logger.info("Checking for duplicates...")

        date_col = 'Date' if 'Date' in self.df.columns else 'date'
        before = len(self.df)

        self.df = self.df.drop_duplicates(subset=[date_col], keep='last')

        removed = before - len(self.df)
        self.cleaning_report['duplicates_removed'] = removed

        if removed:
            logger.warning(f"⚠️ Removed {removed} duplicate rows")
        else:
            logger.info("✅ No duplicates found")

    def _handle_missing_values(self):
        logger.info("Handling missing values...")

        price_cols = ['Open', 'High', 'Low', 'Close'] \
            if 'Open' in self.df.columns else ['open', 'high', 'low', 'close']

        missing = self.df[price_cols].isna().sum().sum()

        if missing > 0:
            logger.warning(f"⚠️ Found {missing} missing values")

            self.df[price_cols] = self.df[price_cols].ffill().bfill()

            before = len(self.df)
            self.df = self.df.dropna(subset=price_cols)
            after = len(self.df)

            self.cleaning_report['missing_values_handled'] = missing

            if before != after:
                logger.warning(f"⚠️ Dropped {before - after} unrecoverable rows")
        else:
            logger.info("✅ No missing values")

    def _validate_price_relationships(self):
        logger.info("Validating price relationships...")

        cols = ['Open', 'High', 'Low', 'Close'] \
            if 'Open' in self.df.columns else ['open', 'high', 'low', 'close']

        open_c, high_c, low_c, close_c = cols

        invalid = self.df[self.df[high_c] < self.df[low_c]]
        if not invalid.empty:
            logger.warning(f"⚠️ Fixing {len(invalid)} rows with High < Low")
            self.df.loc[invalid.index, [high_c, low_c]] = \
                self.df.loc[invalid.index, [low_c, high_c]].values

        logger.info("✅ Price relationships validated")

    def _detect_outliers(self):
        logger.info("Detecting outliers...")

        close_col = 'Close' if 'Close' in self.df.columns else 'close'

        q1 = self.df[close_col].quantile(0.25)
        q3 = self.df[close_col].quantile(0.75)
        iqr = q3 - q1

        lower = q1 - 3 * iqr
        upper = q3 + 3 * iqr

        outliers = self.df[(self.df[close_col] < lower) | (self.df[close_col] > upper)]
        self.cleaning_report['outliers_detected'] = len(outliers)

        if not outliers.empty:
            logger.warning(f"⚠️ Detected {len(outliers)} potential outliers (not removed)")
        else:
            logger.info("✅ No outliers detected")

    def _sort_data(self):
        logger.info("Sorting data by date...")
        date_col = 'Date' if 'Date' in self.df.columns else 'date'
        self.df = self.df.sort_values(by=date_col).reset_index(drop=True)
        logger.info("✅ Data sorted")

    def _log_report(self):
        logger.info("=" * 50)
        logger.info("DATA CLEANING REPORT")
        for k, v in self.cleaning_report.items():
            logger.info(f"{k.replace('_', ' ').title()}: {v}")
        logger.info("=" * 50)

# -----------------------------------------------------------------------------
# Helper / Public Functions
# -----------------------------------------------------------------------------
def clean_gold_data(df: pd.DataFrame):
    """
    Clean gold price data

    Returns:
        (pd.DataFrame, dict)
    """
    cleaner = GoldDataCleaner(df)
    cleaned_df = cleaner.clean()
    return cleaned_df, cleaner.get_cleaning_report()


def save_cleaned_data(df, execution_date, output_dir='/usr/local/airflow/include/data'):
    os.makedirs(output_dir, exist_ok=True)

    filename = f"gold_prices_cleaned_{execution_date}.csv"
    filepath = os.path.join(output_dir, filename)

    df.to_csv(filepath, index=False)
    logger.info(f"✅ Cleaned data saved: {filepath}")

    return filepath


# -----------------------------------------------------------------------------
# Local Test
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    test_df = pd.DataFrame({
        'Date': ['2024-01-01', '2024-01-02', '2024-01-02', '2024-01-03'],
        'Open': [2000, 2010, 2010, None],
        'High': [2020, 2025, 2025, 2040],
        'Low': [1990, 1995, 1995, 2015],
        'Close': [2005, 2015, 2015, 2035],
        'Volume': [100000, 110000, 110000, 120000]
    })

    cleaned, report = clean_gold_data(test_df)
    path = save_cleaned_data(cleaned)

    print(report)
    print(f"Saved to {path}")