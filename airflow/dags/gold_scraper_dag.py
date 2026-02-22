"""
Gold Price Scraper DAG
Schedules daily gold price scraping with error handling and monitoring
"""
from multiprocessing import context

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import pandas as pd
import yfinance as yf
import logging

# Import from the utils package and the scrape function
import sys
sys.path.insert(0, '/usr/local/airflow/include/utils')
from scrape_gold import scrape_gold_prices
from data_cleaner import clean_gold_data
from eda_analyzer import perform_eda

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
}

# Define the DAG
dag = DAG(
    'gold_price_scraper',
    default_args=default_args,
    description='Scrape gold prices daily and save to CSV',
    schedule='@daily',  # Run daily at 9 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['scraping', 'gold', 'finance', 'data-analysis'],
)

def scrape_task_wrapper(**context):
    """Scrape gold prices"""
    logger.info("Starting gold price scraping task...")
    
    try:
        output_dir = '/usr/local/airflow/include/data'
        filepath = scrape_gold_prices(days=365, output_dir=output_dir)
        df = pd.read_csv(filepath)
        
        context['ti'].xcom_push(key='filepath', value=filepath)
        context['ti'].xcom_push(key='dataframe', value=df.to_json())
        
        logger.info(f"✅ Scraping completed: {filepath}")
        return filepath
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def clean_data_task(**context):
    """Clean and preprocess data"""
    logger.info("Starting data cleaning task...")
    
    try:
        df_json = context['ti'].xcom_pull(key='dataframe', task_ids='scrape_gold_prices')
        df = pd.read_json(df_json)
        
        cleaned_df, cleaning_report = clean_gold_data(df)
        
        output_dir = '/usr/local/airflow/include/data'
        cleaned_filename = f"gold_prices_cleaned_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        cleaned_filepath = os.path.join(output_dir, cleaned_filename)
        cleaned_df.to_csv(cleaned_filepath, index=False)
        
        context['ti'].xcom_push(key='cleaned_filepath', value=cleaned_filepath)
        context['ti'].xcom_push(key='cleaned_dataframe', value=cleaned_df.to_json())
        context['ti'].xcom_push(key='cleaning_report', value=cleaning_report)
        
        logger.info(f"✅ Cleaned data saved: {cleaned_filepath}")
        return cleaned_filepath
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def eda_task(**context):
    """Perform exploratory data analysis"""
    logger.info("Starting EDA task...")
    
    
    try:
        from io import StringIO
        
        df_json = context['ti'].xcom_pull(key='cleaned_dataframe', task_ids='clean_data')
        df = pd.read_json(StringIO(df_json))
        
        output_dir = '/usr/local/airflow/include/data/eda'
        stats_report, plot_paths = perform_eda(df, output_dir)
        
        context['ti'].xcom_push(key='eda_stats', value=stats_report)
        context['ti'].xcom_push(key='eda_plots', value=plot_paths)
        
        logger.info(f"✅ EDA complete! Generated {len(plot_paths)} visualizations")
        return stats_report
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def validate_data_task(**context):
    """Validate cleaned data"""
    logger.info("Starting validation...")
    
    try:
        filepath = context['ti'].xcom_pull(key='cleaned_filepath', task_ids='clean_data')
        df = pd.read_csv(filepath)
        
        if len(df) == 0:
            raise ValueError("Empty dataset")
        
        required_columns = ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']
        missing = [col for col in required_columns if col not in df.columns]
        if missing:
            raise ValueError(f"Missing columns: {missing}")
        
        logger.info(f"✅ Validation passed: {len(df)} records")
        return f"Validation successful"
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def generate_summary_task(**context):
    """Generate comprehensive summary"""
    logger.info("Generating summary report...")
    
    try:
        cleaning_report = context['ti'].xcom_pull(key='cleaning_report', task_ids='clean_data')
        eda_stats = context['ti'].xcom_pull(key='eda_stats', task_ids='perform_eda')
        eda_plots = context['ti'].xcom_pull(key='eda_plots', task_ids='perform_eda')
        
        logger.info("=" * 60)
        logger.info("COMPREHENSIVE PIPELINE SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Data Cleaning:")
        logger.info(f"  - Duplicates Removed: {cleaning_report['duplicates_removed']}")
        logger.info(f"  - Missing Values: {cleaning_report['missing_values_handled']}")
        logger.info(f"  - Outliers Detected: {cleaning_report['outliers_detected']}")
        logger.info(f"")
        logger.info(f"EDA Results:")
        logger.info(f"  - Total Records: {eda_stats['summary']['count']}")
        logger.info(f"  - Mean Price: ${eda_stats['summary']['price_statistics']['mean']:,.2f}")
        logger.info(f"  - Total Return: {eda_stats['trends']['total_return_pct']:.2f}%")
        logger.info(f"  - Visualizations: {len(eda_plots)} charts generated")
        logger.info("=" * 60)
        
        return {
            'cleaning': cleaning_report,
            'eda': eda_stats,
            'plots': eda_plots
        }
    except Exception as e:
        logger.error(f"❌ Error: {str(e)}")
        raise

def cleanup_old_files_task(**context):
    """Cleanup old files"""
    logger.info("Starting cleanup...")
    
    data_dir = '/usr/local/airflow/include/data'
    cutoff_date = datetime.now() - timedelta(days=30)
    deleted_count = 0
    
    for filename in os.listdir(data_dir):
        if filename.endswith('.csv'):
            filepath = os.path.join(data_dir, filename)
            if os.path.isfile(filepath):
                file_time = datetime.fromtimestamp(os.path.getmtime(filepath))
                if file_time < cutoff_date:
                    os.remove(filepath)
                    deleted_count += 1
    
    logger.info(f"✅ Cleaned up {deleted_count} old files")
    return f"Cleaned up {deleted_count} files"

# Define tasks - REMOVED provide_context=True
scrape_task = PythonOperator(
    task_id='scrape_gold_prices',
    python_callable=scrape_task_wrapper,
    dag=dag,
)

clean_task = PythonOperator(
    task_id='clean_data',
    python_callable=clean_data_task,
    dag=dag,
)

eda_task_op = PythonOperator(
    task_id='perform_eda',
    python_callable=eda_task,
    dag=dag,
)

validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data_task,
    dag=dag,
)

summary_task = PythonOperator(
    task_id='generate_summary',
    python_callable=generate_summary_task,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files_task,
    dag=dag,
)

# Set task dependencies
scrape_task >> clean_task >> [eda_task_op, validate_task] >> summary_task >> cleanup_task