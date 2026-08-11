"""Daily dbt-only DAG for batch transformations.

Runs dbt models on a daily schedule for data warehouse updates.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator

# Default arguments
default_args = {
    "owner": "analytics",
    "depends_on_past": True,
    "email": ["analytics-team@example.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=10),
}

with DAG(
    "dbt_daily_transformations",
    default_args=default_args,
    description="Daily dbt model runs for analytics",
    schedule_interval="0 6 * * *",  # Daily at 6 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["dbt", "analytics", "daily"],
    max_active_runs=1,
) as dag:

    # Task 1: dbt deps
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command="cd /app/dbt && dbt deps --profiles-dir .",
    )

    # Task 2: dbt seed
    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command="cd /app/dbt && dbt seed --profiles-dir .",
    )

    # Task 3: dbt run
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command="cd /app/dbt && dbt run --profiles-dir .",
    )

    # Task 4: dbt test
    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="cd /app/dbt && dbt test --profiles-dir .",
    )

    # Task 5: dbt docs generate
    dbt_docs = BashOperator(
        task_id="dbt_docs_generate",
        bash_command="cd /app/dbt && dbt docs generate --profiles-dir .",
    )

    # Task 6: Snapshot
    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command="cd /app/dbt && dbt snapshot --profiles-dir .",
    )

    # Task 7: Success notification
    success_email = EmailOperator(
        task_id="send_success_email",
        to=["analytics-team@example.com"],
        subject="dbt Daily Run - Success",
        html_content="""
        <h3>dbt Daily Transformations Complete</h3>
        <p>All models and tests passed successfully.</p>
        <p>Execution Date: {{ ds }}</p>
        """,
    )

    # Dependencies
    dbt_deps >> dbt_seed >> dbt_run >> dbt_test >> dbt_docs >> dbt_snapshot >> success_email
