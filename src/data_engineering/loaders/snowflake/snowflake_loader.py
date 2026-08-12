"""Snowflake data loader."""

from typing import Any

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.loaders.base import BaseLoader, LoaderError
from data_engineering.logger import get_logger


class SnowflakeLoader(BaseLoader):
    """Load data into Snowflake warehouse."""

    def __init__(
        self,
        name: str = "snowflake",
        *,
        table_name: str,
        schema: str = "PUBLIC",
        database: str | None = None,
        warehouse: str | None = None,
        role: str | None = None,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        private_key_path: str | None = None,
        if_exists: str = "append",
        batch_size: int = 10000,
        **kwargs: Any,
    ) -> None:
        """Initialize Snowflake loader.

        Args:
            name: Loader identifier.
            table_name: Target table name.
            schema: Target schema.
            database: Target database.
            warehouse: Snowflake warehouse.
            role: Snowflake role.
            account: Snowflake account.
            user: Snowflake username.
            password: Snowflake password.
            private_key_path: Path to private key file.
            if_exists: Behavior if table exists ("fail", "replace", "append").
            batch_size: Rows per batch.
            **kwargs: Additional configuration.
        """
        super().__init__(
            name,
            table_name=table_name,
            schema=schema,
            database=database,
            warehouse=warehouse,
            role=role,
            account=account,
            user=user,
            password=password,
            private_key_path=private_key_path,
            if_exists=if_exists,
            batch_size=batch_size,
            **kwargs,
        )
        self.table_name = table_name
        self.schema = schema
        self.database = database
        self.warehouse = warehouse
        self.role = role
        self.account = account
        self.user = user
        self.password = password
        self.private_key_path = private_key_path
        self.if_exists = if_exists
        self.batch_size = batch_size
        self.logger = get_logger(f"loader.snowflake.{name}")

    def _get_connection_params(self) -> dict[str, Any]:
        """Build connection parameters."""
        import os

        from cryptography.hazmat.backends import default_backend
        from cryptography.hazmat.primitives import serialization

        params = {
            "account": self.account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": self.user or os.getenv("SNOWFLAKE_USER"),
            "warehouse": self.warehouse
            or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": self.database or os.getenv("SNOWFLAKE_DATABASE"),
            "schema": self.schema,
            "role": self.role or os.getenv("SNOWFLAKE_ROLE"),
        }

        if self.private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            key_path = self.private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH")
            with open(key_path, "rb") as key_file:
                private_key = serialization.load_pem_private_key(
                    key_file.read(),
                    password=os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE", "").encode(),
                    backend=default_backend(),
                )
            params["private_key"] = private_key
        else:
            params["password"] = self.password or os.getenv("SNOWFLAKE_PASSWORD")

        return {k: v for k, v in params.items() if v is not None}

    def validate_connection(self) -> bool:
        """Test Snowflake connection."""
        try:
            conn = snowflake.connector.connect(**self._get_connection_params())
            conn.close()
            return True
        except Exception as e:
            raise LoaderError(f"Cannot connect to Snowflake: {e}") from e

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def load(self, df: pd.DataFrame) -> int:
        """Load DataFrame to Snowflake.

        Args:
            df: DataFrame to load.

        Returns:
            Number of rows loaded.

        Raises:
            LoaderError: If load fails.
        """
        if df.empty:
            self.logger.warning("Empty DataFrame, skipping load")
            return 0

        try:
            self.logger.info(
                "Loading data to Snowflake",
                table=f"{self.database}.{self.schema}.{self.table_name}",
                rows=len(df),
                columns=list(df.columns),
            )

            conn = snowflake.connector.connect(**self._get_connection_params())

            try:
                # Handle table existence
                if self.if_exists == "replace":
                    cursor = conn.cursor()
                    cursor.execute(f"""
                        DROP TABLE IF EXISTS
                        {self.database.upper()}.{self.schema.upper()}.{self.table_name.upper()}
                    """)
                    cursor.close()

                # Write using Snowflake's optimized pandas tool
                success, num_chunks, num_rows, output = write_pandas(
                    conn=conn,
                    df=df,
                    table_name=self.table_name.upper(),
                    database=self.database.upper() if self.database else self.database,
                    schema=self.schema.upper() if self.schema else self.schema,
                    overwrite=(self.if_exists == "replace"),
                    chunk_size=self.batch_size,
                )

                self.logger.info(
                    "Successfully loaded to Snowflake",
                    rows=num_rows,
                    chunks=num_chunks,
                )
                return num_rows

            finally:
                conn.close()

        except snowflake.connector.Error as e:
            raise LoaderError(f"Snowflake load failed: {e}") from e
        except Exception as e:
            raise LoaderError(f"Unexpected error: {e}") from e

    def __enter__(self) -> "SnowflakeLoader":
        """Context manager entry."""
        self.validate_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        if exc_type is not None:
            self.logger.error(
                "Snowflake loader failed",
                exc_info=(exc_type, exc_val, exc_tb),
            )
