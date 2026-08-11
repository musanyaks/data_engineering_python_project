"""Spark ETL Pipeline DAG.

Submits PySpark jobs for large-scale data processing.
Designed to run comfortably on a laptop via Docker.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

from data_engineering.integrations.airflow_utils import get_dag_config
from data_engineering.logger import get_logger

logger = get_logger("airflow.spark_dag")

# DAG configuration
config = get_dag_config(
    dag_id="spark_etl_pipeline",
    schedule=timedelta(hours=1),
    tags=["spark", "etl", "production"],
    start_date=datetime(2024, 1, 1),
)

with DAG(**config) as dag:

    # Task 1: Validate data files exist
    check_files = BashOperator(
        task_id="check_input_files",
        bash_command="""
            if [ ! -f /app/data/sales.csv ]; then
                echo "Input file not found, creating sample data..."
                mkdir -p /app/data
                echo "sale_id,product_id,customer_id,sale_date,quantity,unit_price,total_amount
1,101,1001,2024-01-15,2,29.99,59.98
2,102,1002,2024-01-15,1,49.99,49.99
3,101,1003,2024-01-16,3,29.99,89.97" > /app/data/sales.csv
            fi
            echo "Input files ready"
        """,
    )

    # Task 2: Submit Spark job to process sales
    process_sales = SparkSubmitOperator(
        task_id="process_sales_spark",
        application="/app/spark/jobs/process_sales.py",
        conn_id="spark_default",
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "2g",
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
        },
        verbose=True,
    )

    # Task 3: Submit Spark job to replicate Snowflake data
    replicate_sf = SparkSubmitOperator(
        task_id="replicate_snowflake",
        application="/app/spark/jobs/snowflake_to_postgres.py",
        conn_id="spark_default",
        conf={
            "spark.executor.memory": "2g",
            "spark.executor.cores": "2",
            "spark.driver.memory": "2g",
        },
        verbose=True,
    )

    # Task 4: Run dbt models after Spark processing
    run_dbt = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /app/dbt && dbt run --profiles-dir . --target dev",
    )

    # Task 5: Run dbt tests
    test_dbt = BashOperator(
        task_id="run_dbt_tests",
        bash_command="cd /app/dbt && dbt test --profiles-dir . --target dev",
    )

    # Define dependencies
    check_files >> process_sales >> replicate_sf >> run_dbt >> test_dbt
