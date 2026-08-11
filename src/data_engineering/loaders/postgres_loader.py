"""PostgreSQL data loader."""

from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.loaders.base import BaseLoader, LoaderError
from data_engineering.logger import get_logger


class PostgresLoader(BaseLoader):
    """Load data into PostgreSQL database."""

    def __init__(
        self,
        name: str = "postgres",
        *,
        table_name: str,
        schema: str = "public",
        if_exists: str = "append",
        index: bool = False,
        batch_size: int | None = None,
        connection_string: str | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize PostgreSQL loader.

        Args:
            name: Loader identifier.
            table_name: Target table name.
            schema: Database schema.
            if_exists: Behavior if table exists ("fail", "replace", "append").
            index: Whether to write DataFrame index as column.
            batch_size: Number of rows per batch.
            connection_string: Override default database URL.
            **kwargs: Additional pandas to_sql() arguments.
        """
        super().__init__(
            name,
            table_name=table_name,
            schema=schema,
            if_exists=if_exists,
            index=index,
            batch_size=batch_size,
            connection_string=connection_string,
            **kwargs,
        )
        self.table_name = table_name
        self.schema = schema
        self.if_exists = if_exists
        self.index = index
        self.batch_size = batch_size or get_settings().batch_size
        self.connection_string = connection_string or str(get_settings().database_url)
        self.engine_kwargs = kwargs
        self.logger = get_logger(f"loader.postgres.{name}")
        self._engine: Engine | None = None

    @property
    def engine(self) -> Engine:
        """Lazy-load SQLAlchemy engine."""
        if self._engine is None:
            settings = get_settings()
            self._engine = create_engine(
                self.connection_string,
                pool_size=settings.database_pool_size,
                max_overflow=settings.database_max_overflow,
                pool_pre_ping=True,
            )
        return self._engine

    def validate_connection(self) -> bool:
        """Test database connection.

        Returns:
            True if connection is valid.

        Raises:
            LoaderError: If connection fails.
        """
        try:
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        except Exception as e:
            raise LoaderError(f"Database connection failed: {e}") from e

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def load(self, df: pd.DataFrame) -> int:
        """Load DataFrame to PostgreSQL.

        Args:
            df: DataFrame to load.

        Returns:
            Number of rows loaded.

        Raises:
            LoaderError: If load operation fails.
        """
        if df.empty:
            self.logger.warning("Empty DataFrame, skipping load")
            return 0

        try:
            self.logger.info(
                "Loading data to PostgreSQL",
                table=f"{self.schema}.{self.table_name}",
                rows=len(df),
                columns=list(df.columns),
            )

            # Convert column names to snake_case for SQL compatibility
            df_clean = df.copy()
            df_clean.columns = [
                col.lower().replace(" ", "_").replace("-", "_")
                for col in df_clean.columns
            ]

            rows_loaded = df_clean.to_sql(
                name=self.table_name,
                schema=self.schema,
                con=self.engine,
                if_exists=self.if_exists,
                index=self.index,
                chunksize=self.batch_size,
                method="multi",
                **self.engine_kwargs,
            )

            if rows_loaded is None:
                rows_loaded = len(df_clean)

            self.logger.info(
                "Successfully loaded data",
                table=f"{self.schema}.{self.table_name}",
                rows=rows_loaded,
            )
            return rows_loaded

        except Exception as e:
            raise LoaderError(f"Failed to load data to PostgreSQL: {e}") from e

    def __enter__(self) -> "PostgresLoader":
        """Context manager entry."""
        self.validate_connection()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        if self._engine is not None:
            self._engine.dispose()
            self._engine = None
