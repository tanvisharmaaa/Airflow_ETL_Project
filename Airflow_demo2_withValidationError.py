from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# File paths
RAW_DATA_PATH = '/Users/tanvi/Downloads/country_data.csv'  
CLEANED_DATA_PATH = '/Users/tanvi/Desktop/country_data_cleaned.csv'
VALIDATION_OUTPUT = '/Users/tanvi/Desktop/country_data_validation.txt'
SUMMARY_OUTPUT = '/Users/tanvi/Desktop/country_data_summary.csv'

def load_data():
    df = pd.read_csv(RAW_DATA_PATH)
    print("Data loaded successfully.")
    return df

# Step 2: Data Cleaning and Transformation
def clean_transform_data():
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Basic cleaning and transformations
    required_columns = ['population', 'gdp', 'surface_area']
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"Missing required columns. Available columns: {df.columns}")
    
    # Drop rows with missing critical values
    df.dropna(subset=required_columns, inplace=True)
    
    # Calculate GDP per capita and population density
    df['gdp_per_capita'] = df['gdp'] / df['population']
    df['population_density'] = df['population'] / df['surface_area']
    
    # Calculate regional GDP ratio if 'region' column exists
    if 'region' in df.columns:
        df['regional_gdp_ratio'] = df.groupby('region')['gdp'].transform(lambda x: x / x.sum())
    
    df.to_csv(CLEANED_DATA_PATH, index=False)
    print("Data cleaned and transformed successfully.")

# Step 3: Data Validation
def validate_data():
    df = pd.read_csv(CLEANED_DATA_PATH)
    validation_errors = []
    
    # Check for missing values
    if df.isnull().any().any():
        validation_errors.append("Error: Missing values detected.")
        
    # Ensure non-negative values for key columns
    for col in ['population', 'gdp', 'surface_area']:
        if (df[col] < 0).any():
            validation_errors.append(f"Error: Negative values found in {col} column.")
    
    # Check for outliers in gdp_per_capita
    if (df['gdp_per_capita'] > df['gdp_per_capita'].quantile(0.99)).any():
        validation_errors.append("Warning: High outliers detected in GDP per capita.")
    
    # Save validation results
    with open(VALIDATION_OUTPUT, 'w') as file:
        if validation_errors:
            file.write("\n".join(validation_errors))
            print("Validation failed.")
            return "stop_process"
        else:
            file.write("Validation passed. No errors found.")
            print("Validation passed.")
            return "aggregate_summary"

# Step 4: Generate Summary Report
def aggregate_summary():
    df = pd.read_csv(CLEANED_DATA_PATH)
    
    # Example summary statistics
    summary = {
        "Total Countries": len(df),
        "Average GDP per Capita": df['gdp_per_capita'].mean(),
        "Average Population Density": df['population_density'].mean(),
        "Total GDP": df['gdp'].sum(),
        "Total Population": df['population'].sum(),
    }
    
    # Convert summary dictionary to DataFrame for saving
    summary_df = pd.DataFrame([summary])
    summary_df.to_csv(SUMMARY_OUTPUT, index=False)
    print("Summary report generated and saved.")

# DAG definition
default_args = {
    'owner': 'tanvi',
    'start_date': datetime(2023, 10, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'enhanced_country_data_etl',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    load_data_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data
    )

    clean_transform_data_task = PythonOperator(
        task_id='clean_transform_data',
        python_callable=clean_transform_data
    )

    validate_data_task = BranchPythonOperator(
        task_id='validate_data',
        python_callable=validate_data
    )

    stop_task = PythonOperator(
        task_id='stop_process',
        python_callable=lambda: print("Process stopped due to validation errors.")
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_summary',
        python_callable=aggregate_summary
    )

    # Define task dependencies with conditional branching
    load_data_task >> clean_transform_data_task >> validate_data_task
    validate_data_task >> [stop_task, aggregate_task]