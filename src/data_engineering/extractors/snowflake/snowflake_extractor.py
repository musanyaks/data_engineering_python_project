"""Snowflake data extractor."""

from typing import Any

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.extractors.base import BaseExtractor, ExtractorError
from data_engineering.logger import get_logger


class SnowflakeExtractor(BaseExtractor):
    """Extract data from Snowflake warehouse."""

    def __init__(
        self,
        name: str,
        *,
        query: str,
        warehouse: str | None = None,
        database: str | None = None,
        schema: str | None = None,
        role: str | None = None,
        account: str | None = None,
        user: str | None = None,
        password: str | None = None,
        private_key_path: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize Snowflake extractor.

        Args:
            name: Extractor identifier.
            query: SQL query to execute.
            warehouse: Snowflake warehouse name.
            database: Snowflake database name.
            schema: Snowflake schema name.
            role: Snowflake role.
            account: Snowflake account identifier.
            user: Snowflake username.
            password: Snowflake password.
            private_key_path: Path to private key for key-pair auth.
            **kwargs: Additional configuration.
        """
        super().__init__(
            name,
            query=query,
            warehouse=warehouse,
            database=database,
            schema=schema,
            role=role,
            account=account,
            user=user,
            password=password,
            private_key_path=private_key_path,
            **kwargs,
        )
        self.query = query
        self.warehouse = warehouse
        self.database = database
        self.schema = schema
        self.role = role
        self.account = account
        self.user = user
        self.password = password
        self.private_key_path = private_key_path
        self.logger = get_logger(f"extractor.snowflake.{name}")
        self._connection = None

    def _get_connection_params(self) -> dict[str, Any]:
        """Build connection parameters from config or environment."""
        import os

        params = {
            "account": self.account or os.getenv("SNOWFLAKE_ACCOUNT"),
            "user": self.user or os.getenv("SNOWFLAKE_USER"),
            "warehouse": self.warehouse or os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "database": self.database or os.getenv("SNOWFLAKE_DATABASE"),
            "schema": self.schema or os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "role": self.role or os.getenv("SNOWFLAKE_ROLE"),
        }

        # Authentication: password or key-pair
        if self.private_key_path or os.getenv("SNOWFLAKE_PRIVATE_KEY_PATH"):
            from cryptography.hazmat.backends import default_backend
            from cryptography.hazmat.primitives import serialization

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

        # Remove None values
        return {k: v for k, v in params.items() if v is not None}

    def validate_source(self) -> bool:
        """Test Snowflake connection."""
        try:
            conn = snowflake.connector.connect(**self._get_connection_params())
            conn.close()
            return True
        except Exception as e:
            raise ExtractorError(f"Cannot connect to Snowflake: {e}") from e

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def extract(self) -> pd.DataFrame:
        """Execute query and return DataFrame.

        Returns:
            DataFrame with query results.

        Raises:
            ExtractorError: If query execution fails.
        """
        try:
            self.logger.info(
                "Executing Snowflake query",
                warehouse=self.warehouse,
                database=self.database,
                schema=self.schema,
            )

            conn = snowflake.connector.connect(**self._get_connection_params())

            try:
                df = pd.read_sql(self.query, conn)

                self.logger.info(
                    "Query executed successfully",
                    rows=len(df),
                    columns=list(df.columns),
                )
                return df

            finally:
                conn.close()

        except snowflake.connector.Error as e:
            raise ExtractorError(f"Snowflake query failed: {e}") from e
        except Exception as e:
            raise ExtractorError(f"Unexpected error: {e}") from e

    def __enter__(self) -> "SnowflakeExtractor":
        """Context manager entry."""
        self.validate_source()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        if exc_type is not None:
            self.logger.error(
                "Snowflake extractor failed",
                exc_info=(exc_type, exc_val, exc_tb),
            )
