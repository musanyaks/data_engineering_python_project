"""Main ETL Pipeline DAG.

Orchestrates the full data pipeline from extraction through dbt transformations.
"""

from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup

from data_engineering.config import get_settings
from data_engineering.extractors.csv_extractor import CSVExtractor
from data_engineering.extractors.snowflake.snowflake_extractor import SnowflakeExtractor
from data_engineering.loaders.postgres_loader import PostgresLoader
from data_engineering.loaders.snowflake.snowflake_loader import SnowflakeLoader
from data_engineering.logger import configure_logging
from data_engineering.pipelines.etl_pipeline import ETLPipeline
from data_engineering.transformers.cleaning_transformer import CleaningTransformer

# Configure logging for Airflow
configure_logging()

# Default arguments for all tasks
default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "email": ["data-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

# DAG definition
with DAG(
    "etl_master_pipeline",
    default_args=default_args,
    description="Master ETL pipeline orchestrating extraction, loading, and dbt transformations",
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["etl", "production", "snowflake"],
    max_active_runs=1,
) as dag:

    # Task 1: Extract from CSV sources
    def extract_csv_sources(**context):
        """Extract data from CSV files."""
        pipeline = (
            ETLPipeline("csv_extraction")
            .extract(CSVExtractor("sales", "/app/data/incoming/sales.csv"))
            .transform(CleaningTransformer("clean"))
            .load(PostgresLoader("staging", table_name="staging_sales"))
        )
        metrics = pipeline.run()
        context["ti"].xcom_push(key="csv_metrics", value=metrics)
        return metrics

    extract_csv = PythonOperator(
        task_id="extract_csv_sources",
        python_callable=extract_csv_sources,
    )

    # Task 2: Extract from Snowflake
    def extract_snowflake_sources(**context):
        """Extract data from Snowflake warehouse."""
        extractor = SnowflakeExtractor(
            name="sf_events",
            query="""
                SELECT
                    event_id,
                    user_id,
                    event_type,
                    event_timestamp,
                    session_id,
                    page_url,
                    device_type,
                    country,
                    amount
                FROM analytics.raw.kafka_events
                WHERE event_timestamp >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
            """,
        )
        df = extractor.extract()

        loader = SnowflakeLoader(
            name="sf_staging",
            table_name="staging_events",
            schema="staging",
            if_exists="append",
        )
        rows = loader(df)

        context["ti"].xcom_push(key="sf_rows", value=rows)
        return rows

    extract_snowflake = PythonOperator(
        task_id="extract_snowflake_sources",
        python_callable=extract_snowflake_sources,
    )

    # Task 3: Run dbt models
    def run_dbt_transformations(**context):
        """Execute dbt models for transformations."""
        import subprocess

        result = subprocess.run(
            ["dbt", "run", "--profiles-dir", "/app/dbt", "--project-dir", "/app/dbt"],
            capture_output=True,
            text=True,
            check=True,
        )

        context["ti"].xcom_push(key="dbt_output", value=result.stdout)
        return result.stdout

    run_dbt = PythonOperator(
        task_id="run_dbt_models",
        python_callable=run_dbt_transformations,
    )

    # Task 4: Run dbt tests
    def run_dbt_tests(**context):
        """Execute dbt tests for data quality."""
        import subprocess

        result = subprocess.run(
            ["dbt", "test", "--profiles-dir", "/app/dbt", "--project-dir", "/app/dbt"],
            capture_output=True,
            text=True,
            check=True,
        )

        context["ti"].xcom_push(key="dbt_test_output", value=result.stdout)
        return result.stdout

    test_dbt = PythonOperator(
        task_id="run_dbt_tests",
        python_callable=run_dbt_tests,
    )

    # Task 5: Data quality checks
    def run_data_quality_checks(**context):
        """Run custom data quality validations."""
        from data_engineering.utils.validators import check_nulls
        import pandas as pd
        from sqlalchemy import create_engine

        settings = get_settings()
        engine = create_engine(str(settings.database_url))

        # Check staging tables
        with engine.connect() as conn:
            df = pd.read_sql("SELECT * FROM staging_sales LIMIT 1000", conn)

        high_nulls = check_nulls(df, threshold=0.05)

        if high_nulls:
            raise ValueError(f"Data quality check failed. High nulls: {high_nulls}")

        return "Data quality checks passed"

    quality_check = PythonOperator(
        task_id="data_quality_checks",
        python_callable=run_data_quality_checks,
    )

    # Task 6: Load to Snowflake marts
    def load_to_snowflake_marts(**context):
        """Load transformed data to Snowflake marts."""
        pipeline = (
            ETLPipeline("sf_mart_load")
            .extract(
                SnowflakeExtractor(
                    name="transformed_data",
                    query="SELECT * FROM analytics.fct_sales",
                )
            )
            .load(
                SnowflakeLoader(
                    name="sf_mart",
                    table_name="fct_sales",
                    schema="marts",
                    if_exists="replace",
                )
            )
        )
        metrics = pipeline.run()
        return metrics

    load_marts = PythonOperator(
        task_id="load_snowflake_marts",
        python_callable=load_to_snowflake_marts,
    )

    # Define task dependencies
    [extract_csv, extract_snowflake] >> quality_check >> run_dbt >> test_dbt >> load_marts
