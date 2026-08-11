"""PySpark-based data loaders."""

from typing import Any

from pyspark.sql import DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.logger import get_logger


class SparkPostgresLoader:
    """Load Spark DataFrame to PostgreSQL using JDBC.

    Example:
        loader = SparkPostgresLoader("pg", table_name="staging_sales")
        loader.load(df)
    """

    def __init__(
        self,
        name: str = "postgres",
        *,
        table_name: str,
        schema: str = "public",
        mode: str = "append",
        jdbc_url: str | None = None,
        user: str | None = None,
        password: str | None = None,
        batch_size: int = 1000,
    ) -> None:
        """Initialize Spark PostgreSQL loader.

        Args:
            name: Loader identifier.
            table_name: Target table name.
            schema: Database schema.
            mode: Write mode ("append", "overwrite", "ignore", "error").
            jdbc_url: JDBC connection URL.
            user: Database username.
            password: Database password.
            batch_size: Batch size for writes.
        """
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.mode = mode
        self.batch_size = batch_size
        self.logger = get_logger(f"spark.loader.postgres.{name}")

        import os
        self.jdbc_url = jdbc_url or os.getenv(
            "DATABASE_URL", "jdbc:postgresql://localhost:5432/dbname"
        ).replace("postgresql://", "jdbc:postgresql://")
        self.user = user or os.getenv("POSTGRES_USER", "postgres")
        self.password = password or os.getenv("POSTGRES_PASSWORD", "postgres")

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def load(self, df: DataFrame) -> int:
        """Load DataFrame to PostgreSQL.

        Args:
            df: Spark DataFrame to load.

        Returns:
            Number of rows loaded.
        """
        row_count = df.count()
        if row_count == 0:
            self.logger.warning("Empty DataFrame, skipping load")
            return 0

        self.logger.info(
            "Loading to PostgreSQL",
            table=f"{self.schema}.{self.table_name}",
            rows=row_count,
        )

        (
            df.write
            .format("jdbc")
            .option("url", self.jdbc_url)
            .option("dbtable", f"{self.schema}.{self.table_name}")
            .option("user", self.user)
            .option("password", self.password)
            .option("batchsize", self.batch_size)
            .option("driver", "org.postgresql.Driver")
            .mode(self.mode)
            .save()
        )

        self.logger.info(
            "Successfully loaded to PostgreSQL",
            rows=row_count,
        )
        return row_count

    def __call__(self, df: DataFrame) -> int:
        """Allow loader to be called as a function."""
        return self.load(df)


class SparkSnowflakeLoader:
    """Load Spark DataFrame to Snowflake.

    Uses the Snowflake Spark Connector for efficient writes.
    """

    def __init__(
        self,
        name: str = "snowflake",
        *,
        table_name: str,
        schema: str = "PUBLIC",
        mode: str = "append",
        sf_options: dict[str, str] | None = None,
    ) -> None:
        """Initialize Spark Snowflake loader.

        Args:
            name: Loader identifier.
            table_name: Target table name.
            schema: Target schema.
            mode: Write mode ("append", "overwrite", "ignore", "error").
            sf_options: Snowflake connection options.
        """
        self.name = name
        self.table_name = table_name
        self.schema = schema
        self.mode = mode
        self.sf_options = sf_options or self._default_options()
        self.logger = get_logger(f"spark.loader.snowflake.{name}")

    def _default_options(self) -> dict[str, str]:
        """Build default Snowflake options from environment."""
        import os
        return {
            "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
            "sfUser": os.getenv("SNOWFLAKE_USER", ""),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", ""),
            "sfSchema": self.schema,
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
        }

    def load(self, df: DataFrame) -> int:
        """Load DataFrame to Snowflake.

        Args:
            df: Spark DataFrame to load.

        Returns:
            Number of rows loaded.
        """
        row_count = df.count()
        if row_count == 0:
            self.logger.warning("Empty DataFrame, skipping load")
            return 0

        self.logger.info(
            "Loading to Snowflake",
            table=f"{self.schema}.{self.table_name}",
            rows=row_count,
        )

        (
            df.write
            .format("snowflake")
            .options(**self.sf_options)
            .option("dbtable", self.table_name)
            .mode(self.mode)
            .save()
        )

        self.logger.info(
            "Successfully loaded to Snowflake",
            rows=row_count,
        )
        return row_count

    def __call__(self, df: DataFrame) -> int:
        """Allow loader to be called as a function."""
        return self.load(df)
