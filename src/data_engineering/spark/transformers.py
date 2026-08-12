"""PySpark-based data transformers."""

from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, trim

from data_engineering.logger import get_logger


class SparkCleaningTransformer:
    """Clean and standardize Spark DataFrames.

    Equivalent to CleaningTransformer but for Spark DataFrames.
    Optimized for laptop use with adaptive query execution.

    Example:
        transformer = SparkCleaningTransformer("clean")
        clean_df = transformer.transform(df)
    """

    def __init__(
        self,
        name: str = "clean",
        *,
        drop_duplicates: bool = True,
        duplicate_columns: list[str] | None = None,
        fill_null_strategy: str = "drop",
        fill_value: Any = None,
        trim_strings: bool = True,
        lowercase_columns: bool = True,
    ) -> None:
        """Initialize Spark cleaning transformer.

        Args:
            name: Transformer identifier.
            drop_duplicates: Whether to remove duplicate rows.
            duplicate_columns: Columns to consider for duplicates.
            fill_null_strategy: How to handle nulls ("drop", "fill").
            fill_value: Value to use when fill_null_strategy is "fill".
            trim_strings: Whether to trim whitespace from strings.
            lowercase_columns: Whether to lowercase column names.
        """
        self.name = name
        self.drop_duplicates = drop_duplicates
        self.duplicate_columns = duplicate_columns
        self.fill_null_strategy = fill_null_strategy
        self.fill_value = fill_value
        self.trim_strings = trim_strings
        self.lowercase_columns = lowercase_columns
        self.logger = get_logger(f"spark.transformer.{name}")

    def transform(self, df: DataFrame) -> DataFrame:
        """Apply cleaning transformations.

        Args:
            df: Input Spark DataFrame.

        Returns:
            Cleaned Spark DataFrame.
        """
        result = df

        # Lowercase column names
        if self.lowercase_columns:
            for old_name in result.columns:
                new_name = old_name.lower().strip().replace(" ", "_")
                if new_name != old_name:
                    result = result.withColumnRenamed(old_name, new_name)
            self.logger.debug("Lowercased column names")

        # Trim string columns
        if self.trim_strings:
            for field in result.schema.fields:
                if str(field.dataType) == "StringType()":
                    result = result.withColumn(
                        field.name,
                        trim(col(field.name)),
                    )
            self.logger.debug("Trimmed string columns")

        # Drop duplicates
        if self.drop_duplicates:
            before = result.count()
            if self.duplicate_columns:
                result = result.dropDuplicates(subset=self.duplicate_columns)
            else:
                result = result.dropDuplicates()
            after = result.count()
            if before != after:
                self.logger.info(f"Dropped {before - after} duplicate rows")

        # Handle nulls
        if self.fill_null_strategy == "drop":
            before = result.count()
            result = result.dropna()
            after = result.count()
            if before != after:
                self.logger.info(f"Dropped {before - after} rows with nulls")
        elif self.fill_null_strategy == "fill":
            result = result.fillna(self.fill_value)
            self.logger.info("Filled nulls with specified value")

        self.logger.info(
            "Cleaning complete",
            input_rows=df.count(),
            output_rows=result.count(),
        )
        return result

    def __call__(self, df: DataFrame) -> DataFrame:
        """Allow transformer to be called as a function."""
        return self.transform(df)
