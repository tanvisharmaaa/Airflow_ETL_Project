from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# File paths
RAW_DATA_PATH = '/Users/tanvi/Downloads/country_data.csv'  
CLEANED_DATA_PATH = '/Users/tanvi/Desktop/country_data_cleaned_fixed.csv'
VALIDATION_OUTPUT = '/Users/tanvi/Desktop/country_data_validation_fixed.txt'
SUMMARY_OUTPUT = '/Users/tanvi/Desktop/country_data_summary_fixed.csv'

# Step 1: Load Data
def load_data():
    df = pd.read_csv(RAW_DATA_PATH)
    print("Data loaded successfully.")
    return df

# Step 2: Data Cleaning and Transformation with Outlier Handling
def clean_transform_data():
    df = pd.read_csv(RAW_DATA_PATH)
    
    # Check that required columns are present
    required_columns = ['population', 'gdp', 'surface_area']
    if not all(col in df.columns for col in required_columns):
        raise KeyError(f"Missing required columns. Available columns: {df.columns}")
    
    # Fill missing values in critical columns with median values
    for col in required_columns:
        if df[col].isnull().any():
            median_value = df[col].median()
            df[col].fillna(median_value, inplace=True)
            print(f"Filled missing values in '{col}' with median value: {median_value}")
    
    # Fill missing values in all other columns with forward fill, backward fill, or a default value
    df.fillna(method='ffill', inplace=True)  # Forward fill as an example
    df.fillna(method='bfill', inplace=True)  # Backward fill for any remaining NaNs
    
    # Calculate GDP per capita and population density
    df['gdp_per_capita'] = df['gdp'] / df['population']
    df['population_density'] = df['population'] / df['surface_area']
    
    # Cap gdp_per_capita at the 99th percentile to handle outliers
    cap_value = df['gdp_per_capita'].quantile(0.99)
    df['gdp_per_capita'] = np.where(df['gdp_per_capita'] > cap_value, cap_value, df['gdp_per_capita'])
    
    # Calculate regional GDP ratio if 'region' column exists
    if 'region' in df.columns:
        df['regional_gdp_ratio'] = df.groupby('region')['gdp'].transform(lambda x: x / x.sum())
    
    df.to_csv(CLEANED_DATA_PATH, index=False)
    print("Data cleaned, transformed, and missing values handled successfully.")



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
    df = pd.read_csv('/Users/tanvi/Desktop/country_data_cleaned_fixed.csv')
    
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
    'enhanced_country_data_etl_with_outlier_handling',
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
