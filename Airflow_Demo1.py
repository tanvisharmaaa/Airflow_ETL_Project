from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.dates import days_ago
from datetime import timedelta
import pandas as pd
import numpy as np
import os

# File paths for input data and output results
DATA_PATH = '/Users/tanvi/Desktop/nifty_50.csv'
CLEANED_DATA_PATH = '/Users/tanvi/Desktop/cleaned_stock_data.csv'
DAILY_RETURNS_OUTPUT = '/Users/tanvi/Desktop/stock_daily_returns.csv'
WEEKLY_RETURNS_OUTPUT = '/Users/tanvi/Desktop/stock_weekly_returns.csv'
VOLATILITY_OUTPUT = '/Users/tanvi/Desktop/stock_volatility.csv'
MOVING_AVERAGES_OUTPUT = '/Users/tanvi/Desktop/stock_moving_averages.csv'
VALIDATION_OUTPUT = '/Users/tanvi/Desktop/stock_data_validation.txt'
FINAL_AGGREGATE_OUTPUT = '/Users/tanvi/Desktop/final_aggregate_summary.csv'

def clean_data():
    df = pd.read_csv(DATA_PATH)
    # Example cleaning: fill missing values
    df.fillna(method='ffill', inplace=True)
    df.to_csv(CLEANED_DATA_PATH, index=False)
    print("Data cleaned and saved.")

# Step 2: Calculate Daily Returns
def calculate_daily_returns():
    df = pd.read_csv(CLEANED_DATA_PATH)
    df['daily_return'] = df.groupby('Index Name')['Close'].pct_change()
    df[['Index Name', 'Date', 'daily_return']].to_csv(DAILY_RETURNS_OUTPUT, index=False)
    print("Daily returns calculated and saved.")

# Step 3: Calculate Weekly Returns
def calculate_weekly_returns():
    df = pd.read_csv(CLEANED_DATA_PATH)
    df['weekly_return'] = df.groupby('Index Name')['Close'].pct_change(periods=5)
    df[['Index Name', 'Date', 'weekly_return']].to_csv(WEEKLY_RETURNS_OUTPUT, index=False)
    print("Weekly returns calculated and saved.")

# Step 4: Calculate Volatility (7-day and 30-day rolling window)
def calculate_volatility():
    df = pd.read_csv(DAILY_RETURNS_OUTPUT)
    df['7_day_volatility'] = df.groupby('Index Name')['daily_return'].rolling(7).std().reset_index(0, drop=True)
    df['30_day_volatility'] = df.groupby('Index Name')['daily_return'].rolling(30).std().reset_index(0, drop=True)
    df[['Index Name', 'Date', '7_day_volatility', '30_day_volatility']].to_csv(VOLATILITY_OUTPUT, index=False)
    print("Volatility calculated and saved.")

# Step 5: Calculate Moving Averages (7-day and 30-day)
def calculate_moving_averages():
    df = pd.read_csv(CLEANED_DATA_PATH)
    df['7_day_moving_avg'] = df.groupby('Index Name')['Close'].transform(lambda x: x.rolling(7).mean())
    df['30_day_moving_avg'] = df.groupby('Index Name')['Close'].transform(lambda x: x.rolling(30).mean())
    df[['Index Name', 'Date', '7_day_moving_avg', '30_day_moving_avg']].to_csv(MOVING_AVERAGES_OUTPUT, index=False)
    print("Moving averages calculated and saved.")

# Step 6: Data Validation
def validate_data():
    df = pd.read_csv(CLEANED_DATA_PATH)
    validation_errors = []
    
    # Check for missing values
    if df.isnull().any().any():
        validation_errors.append("Error: Missing values detected.")
        
    # Ensure non-negative values for price and volume columns
    for col in ['Open', 'High', 'Low', 'Close']:
        if (df[col] < 0).any():
            validation_errors.append(f"Error: Negative values found in {col} column.")
    
    # Check for duplicate dates per index
    duplicate_dates = df.duplicated(subset=['Index Name', 'Date']).any()
    if duplicate_dates:
        validation_errors.append("Error: Duplicate dates detected in Index Name and Date combination.")
    
    # Write validation results to file
    with open(VALIDATION_OUTPUT, 'w') as file:
        if validation_errors:
            file.write("\n".join(validation_errors))
            print("Validation failed.")
            return "stop_process"
        else:
            file.write("Validation passed.")
            print("Validation passed.")
            return "aggregate_summary"

# Final Aggregation
def aggregate_summary():
    daily_returns = pd.read_csv(DAILY_RETURNS_OUTPUT)
    weekly_returns = pd.read_csv(WEEKLY_RETURNS_OUTPUT)
    volatility = pd.read_csv(VOLATILITY_OUTPUT)
    moving_averages = pd.read_csv(MOVING_AVERAGES_OUTPUT)
    
    # Example aggregation: merge on Index Name and Date
    result = daily_returns.merge(weekly_returns, on=['Index Name', 'Date'], how='left')
    result = result.merge(volatility, on=['Index Name', 'Date'], how='left')
    result = result.merge(moving_averages, on=['Index Name', 'Date'], how='left')
    result.to_csv(FINAL_AGGREGATE_OUTPUT, index=False)
    print("Final aggregate summary saved.")

# DAG definition
default_args = {
    'owner': 'tanvi',
    'start_date': days_ago(1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('enhanced_stock_etl_dag', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:
    clean_data_task = PythonOperator(task_id='clean_data', python_callable=clean_data)
    daily_returns_task = PythonOperator(task_id='calculate_daily_returns', python_callable=calculate_daily_returns)
    weekly_returns_task = PythonOperator(task_id='calculate_weekly_returns', python_callable=calculate_weekly_returns)
    volatility_task = PythonOperator(task_id='calculate_volatility', python_callable=calculate_volatility)
    moving_averages_task = PythonOperator(task_id='calculate_moving_averages', python_callable=calculate_moving_averages)
    validation_task = BranchPythonOperator(task_id='validate_data', python_callable=validate_data)
    stop_task = PythonOperator(task_id='stop_process', python_callable=lambda: print("Process stopped due to validation errors"))
    aggregate_task = PythonOperator(task_id='aggregate_summary', python_callable=aggregate_summary)

    # Define task dependencies
    clean_data_task >> [daily_returns_task, weekly_returns_task] >> volatility_task
    clean_data_task >> moving_averages_task
    volatility_task >> validation_task
    moving_averages_task >> validation_task
    validation_task >> [stop_task, aggregate_task]
