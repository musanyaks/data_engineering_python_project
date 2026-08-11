"""Shared helpers for building Airflow DAG configuration."""

from datetime import datetime, timedelta
from typing import Any, Optional, Union


def get_dag_config(
    dag_id: str,
    schedule: Union[str, timedelta, None] = None,
    tags: Optional[list] = None,
    start_date: Optional[datetime] = None,
    retries: int = 2,
    retry_delay: timedelta = timedelta(minutes=5),
    catchup: bool = False,
    **extra: Any,
) -> dict:
    """Build a standard kwargs dict for instantiating an Airflow DAG.

    Args:
        dag_id: Unique DAG identifier.
        schedule: Cron string, timedelta, or None for manual triggering.
        tags: List of tags shown in the Airflow UI.
        start_date: DAG start date. Defaults to 2024-01-01 if not given.
        retries: Default task retries.
        retry_delay: Delay between retries.
        catchup: Whether Airflow should backfill missed runs.
        **extra: Any additional DAG() kwargs to pass through.

    Returns:
        dict: kwargs ready to unpack into DAG(**config).
    """
    default_args = {
        "owner": "data_engineering",
        "retries": retries,
        "retry_delay": retry_delay,
    }
    default_args.update(extra.pop("default_args", {}))

    config = {
        "dag_id": dag_id,
        "schedule": schedule,
        "start_date": start_date or datetime(2024, 1, 1),
        "catchup": catchup,
        "tags": tags or [],
        "default_args": default_args,
    }
    config.update(extra)
    return config