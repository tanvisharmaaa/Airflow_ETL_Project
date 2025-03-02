# Apache Airflow ETL Project

## Overview
This project implements **ETL (Extract, Transform, Load) pipelines** using **Apache Airflow** to automate data processing tasks. The project includes:
- **Stock Market Data Pipeline**: Processes financial stock market data for analysis.
- **Country Data Analysis Pipeline**: ETL workflow for country-level statistical analysis.

Apache Airflow is used to **orchestrate tasks, manage dependencies, and automate data processing** efficiently.

---

## Features
- **Data Extraction**: Reads raw stock market and country data from CSV files.
- **Data Cleaning & Transformation**: Applies validation, filters out errors, and processes data.
- **Feature Engineering**: Computes **returns, moving averages, and volatility** for stock market data.
- **Task Orchestration**: Uses Apache Airflow DAGs to schedule and automate workflows.
- **Error Handling & Data Validation**: Identifies missing values, outliers, and validation errors.

---

## Project Structure
```
├── airflow_dags/
│   ├── Airflow_Demo1.py                 # Stock Market ETL Pipeline DAG
│   ├── Airflow_demo2_withValidationError.py # Country Data ETL Pipeline (with validation issue)
│   ├── Demo2_fixed.py                    # Fixed version of Country Data ETL Pipeline
│── data/
│   ├── nifty_50.csv                      # Stock market data
│   ├── country_data.csv                   # Country dataset
```

---

## Installation & Setup
### 1. Install Apache Airflow
Ensure you have Python **3.7+** installed.

```sh
pip install apache-airflow
```

To install additional dependencies for database connections:
```sh
pip install apache-airflow[postgres]
```

### 2. Initialize Airflow
Set up the Airflow home directory and initialize the database:
```sh
export AIRFLOW_HOME=~/airflow
airflow db init
```

### 3. Start Airflow
Run the scheduler and web server:
```sh
airflow scheduler &
airflow webserver --port 8080
```
Open **Airflow UI** at [http://localhost:8080](http://localhost:8080) in your browser.

---

## ETL Pipelines
### 1. Stock Market Data Pipeline
**Goal:** Automate financial data processing for stock market analysis.

**DAG File:** `Airflow_Demo1.py`  
**Dataset:** `nifty_50.csv`

**Workflow Steps:**
1. **Extract**: Load stock market data from `nifty_50.csv`.
2. **Transform**: Compute **daily & weekly returns**, **volatility**, and **moving averages**.
3. **Validate**: Ensure data consistency and remove errors.
4. **Load**: Store processed data in output files.

**Outputs:**
- `daily_returns.csv`
- `weekly_returns.csv`
- `volatility.csv`
- `moving_averages.csv`

---

### 2. Country Data Analysis Pipeline
**Goal:** Perform ETL on country statistics with validation.

**DAG File:** `Airflow_demo2_withValidationError.py`  
**Dataset:** `country_data.csv`

**Workflow Steps:**
1. **Extract**: Load country-level data.
2. **Transform**: Clean and normalize country statistics.
3. **Validate**: Detect missing values and outliers.
4. **Aggregate**: Compute summary statistics for analysis.

**Error Handling:**
- The initial pipeline had a **validation issue**, preventing execution.
- **Fixed Version:** `Demo2_fixed.py` resolves the issue.

**Outputs:**
- `cleaned_country_data.csv`
- `aggregated_country_metrics.csv`

---

## How to Run the DAGs
1. Copy the DAG scripts into Airflow’s DAGs directory:
   ```sh
   cp airflow_dags/*.py ~/airflow/dags/
   ```
2. Restart the Airflow scheduler & webserver:
   ```sh
   airflow scheduler &
   airflow webserver --port 8080
   ```
3. Open **Airflow UI** and manually trigger the DAGs.

---

## Airflow DAGs Overview
### Stock Market Data DAG Workflow
```
[Data Extraction] → [Data Cleaning] → [Feature Engineering] → [Validation] → [Storage]
```
### Country Data Analysis DAG Workflow
```
[Load Data] → [Cleaning & Transformation] → [Validation] → [Aggregation] → [Storage]
```

---

## Troubleshooting
- Check DAG status in the **Airflow UI** (`http://localhost:8080`).
- View logs for debugging:
  ```sh
  airflow tasks logs <dag_id> <task_id> <execution_date>
  ```
- Ensure Airflow scheduler is running:
  ```sh
  airflow scheduler
  ```


