from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import snowflake.connector
from scripts.etl_sp500 import fetch_transform_load

# Snowflake connection details (fill with actual secure values)
SNOWFLAKE_CONFIG = {
    'user': '--',
    'password': '--',
    'account': '---',
    'warehouse': '--',
    'database': '--',
    'schema': '--'
}

FINAL_TABLE = '--'

def load_to_snowflake():
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cs = conn.cursor()

    cs.execute(f"""
        CREATE TABLE IF NOT EXISTS {FINAL_TABLE} (
            trade_date TIMESTAMP,
            close_price FLOAT,
            high_price FLOAT,
            low_price FLOAT,
            open_price FLOAT,
            volume FLOAT,
            symbol STRING,
            close_change FLOAT,
            close_pct_change FLOAT
        );
    """)

    with open("data/stock_prices.csv", "r") as f:
        next(f)  # Skip header
        for line in f:
            values = [x.strip() for x in line.split(",")]
            placeholders = ','.join(['%s'] * len(values))
            insert_query = f"""
                INSERT INTO {FINAL_TABLE} (
                    trade_date, close_price, high_price, low_price, open_price, volume,
                    symbol, close_change, close_pct_change
                ) VALUES ({placeholders})
            """
            cs.execute(insert_query, values)

    cs.close()
    conn.close()

# Define the DAG
with DAG(
    dag_id='sp500_to_snowflake_dag',
    start_date=datetime(2023, 1, 1),
    schedule_interval='*/30 * * * *',  # every 30 minutes
    catchup=False,
    tags=['stock', 'snowflake']
) as dag:

    task_fetch_transform = PythonOperator(
        task_id='fetch_transform_task',
        python_callable=fetch_transform_load
    )

    task_load_to_snowflake = PythonOperator(
        task_id='load_to_snowflake_task',
        python_callable=load_to_snowflake
    )

    task_fetch_transform >> task_load_to_snowflake
