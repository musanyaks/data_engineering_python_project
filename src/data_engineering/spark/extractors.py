"""PySpark-based data extractors."""

from typing import Any

from pyspark.sql import DataFrame
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.logger import get_logger
from data_engineering.spark.spark_session import get_spark_session


class SparkCSVExtractor:
    """Extract CSV data using PySpark.

    Optimized for laptop use with small-to-medium datasets.
    For large files, Spark handles partitioning automatically.

    Example:
        extractor = SparkCSVExtractor("sales", "data/sales.csv")
        df = extractor.extract()
    """

    def __init__(
        self,
        name: str,
        file_path: str,
        *,
        header: bool = True,
        infer_schema: bool = True,
        delimiter: str = ",",
        **options: Any,
    ) -> None:
        """Initialize Spark CSV extractor.

        Args:
            name: Extractor identifier.
            file_path: Path to CSV file.
            header: Whether CSV has header row.
            infer_schema: Whether to infer data types.
            delimiter: Field delimiter.
            **options: Additional Spark read options.
        """
        self.name = name
        self.file_path = file_path
        self.header = header
        self.infer_schema = infer_schema
        self.delimiter = delimiter
        self.options = options
        self.logger = get_logger(f"spark.extractor.csv.{name}")
        self.spark = get_spark_session()

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def extract(self) -> DataFrame:
        """Read CSV into Spark DataFrame.

        Returns:
            Spark DataFrame.
        """
        self.logger.info(
            "Reading CSV with Spark",
            path=self.file_path,
            header=self.header,
        )

        df = (
            self.spark.read
            .option("header", str(self.header).lower())
            .option("inferSchema", str(self.infer_schema).lower())
            .option("delimiter", self.delimiter)
            .options(**self.options)
            .csv(self.file_path)
        )

        row_count = df.count()
        self.logger.info(
            "CSV loaded",
            rows=row_count,
            columns=len(df.columns),
            schema=str(df.schema.simpleString()),
        )
        return df


class SparkSnowflakeExtractor:
    """Extract data from Snowflake using PySpark.

    Uses the Snowflake Spark Connector for efficient reads.
    """

    def __init__(
        self,
        name: str,
        *,
        query: str,
        sf_options: dict[str, str] | None = None,
    ) -> None:
        """Initialize Spark Snowflake extractor.

        Args:
            name: Extractor identifier.
            query: SQL query to execute.
            sf_options: Snowflake connection options.
        """
        self.name = name
        self.query = query
        self.sf_options = sf_options or self._default_options()
        self.logger = get_logger(f"spark.extractor.snowflake.{name}")
        self.spark = get_spark_session()

    def _default_options(self) -> dict[str, str]:
        """Build default Snowflake options from environment."""
        import os
        return {
            "sfURL": f"{os.getenv('SNOWFLAKE_ACCOUNT')}.snowflakecomputing.com",
            "sfUser": os.getenv("SNOWFLAKE_USER", ""),
            "sfPassword": os.getenv("SNOWFLAKE_PASSWORD", ""),
            "sfDatabase": os.getenv("SNOWFLAKE_DATABASE", ""),
            "sfSchema": os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC"),
            "sfWarehouse": os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
            "sfRole": os.getenv("SNOWFLAKE_ROLE", ""),
        }

    def extract(self) -> DataFrame:
        """Read from Snowflake into Spark DataFrame.

        Returns:
            Spark DataFrame.
        """
        self.logger.info(
            "Reading from Snowflake",
            query=self.query[:100],
        )

        df = (
            self.spark.read
            .format("snowflake")
            .options(**self.sf_options)
            .option("query", self.query)
            .load()
        )

        row_count = df.count()
        self.logger.info(
            "Snowflake data loaded",
            rows=row_count,
            columns=len(df.columns),
        )
        return df
