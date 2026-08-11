"""Data cleaning transformer."""

from typing import Any, List, Optional

import pandas as pd

from data_engineering.transformers.base import BaseTransformer, TransformerError


class CleaningTransformer(BaseTransformer):
    """Clean and standardize DataFrame data.

    Supports removing duplicates, handling nulls, trimming strings,
    and renaming columns.
    """

    def __init__(
        self,
        name: str = "cleaning",
        *,
        drop_duplicates: bool = True,
        duplicate_subset: Optional[List[str]] = None,
        fill_null_strategy: str = "drop",
        fill_value: Any = None,
        trim_strings: bool = True,
        lowercase_columns: bool = True,
        rename_map: Optional[dict[str, str]] = None,
        **kwargs: Any,
    ) -> None:
        """Initialize cleaning transformer.

        Args:
            name: Transformer identifier.
            drop_duplicates: Whether to remove duplicate rows.
            duplicate_subset: Columns to consider for duplicates.
            fill_null_strategy: How to handle nulls ("drop", "fill", "ffill", "bfill").
            fill_value: Value to use when fill_null_strategy is "fill".
            trim_strings: Whether to trim whitespace from strings.
            lowercase_columns: Whether to lowercase column names.
            rename_map: Dictionary mapping old column names to new ones.
            **kwargs: Additional configuration.
        """
        super().__init__(
            name,
            drop_duplicates=drop_duplicates,
            duplicate_subset=duplicate_subset,
            fill_null_strategy=fill_null_strategy,
            fill_value=fill_value,
            trim_strings=trim_strings,
            lowercase_columns=lowercase_columns,
            rename_map=rename_map,
            **kwargs,
        )
        self.drop_duplicates = drop_duplicates
        self.duplicate_subset = duplicate_subset
        self.fill_null_strategy = fill_null_strategy
        self.fill_value = fill_value
        self.trim_strings = trim_strings
        self.lowercase_columns = lowercase_columns
        self.rename_map = rename_map or {}

    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply cleaning transformations.

        Args:
            df: Input DataFrame.

        Returns:
            Cleaned DataFrame.
        """
        result = df.copy()

        # Rename columns
        if self.rename_map:
            result = result.rename(columns=self.rename_map)
            self.logger.debug("Renamed columns", mapping=self.rename_map)

        # Lowercase column names
        if self.lowercase_columns:
            result.columns = result.columns.str.lower().str.strip()
            self.logger.debug("Lowercased column names")

        # Trim string values
        if self.trim_strings:
            str_cols = result.select_dtypes(include=["object"]).columns
            for col in str_cols:
                result[col] = result[col].astype(str).str.strip()
            self.logger.debug("Trimmed string columns", columns=list(str_cols))

        # Handle duplicates
        if self.drop_duplicates:
            before_count = len(result)
            result = result.drop_duplicates(subset=self.duplicate_subset)
            dropped = before_count - len(result)
            if dropped > 0:
                self.logger.info(f"Dropped {dropped} duplicate rows")

        # Handle nulls
        if self.fill_null_strategy == "drop":
            before_count = len(result)
            result = result.dropna()
            dropped = before_count - len(result)
            if dropped > 0:
                self.logger.info(f"Dropped {dropped} rows with nulls")
        elif self.fill_null_strategy == "fill":
            result = result.fillna(self.fill_value)
            self.logger.info("Filled nulls with specified value")
        elif self.fill_null_strategy == "ffill":
            result = result.fillna(method="ffill")
            self.logger.info("Forward-filled nulls")
        elif self.fill_null_strategy == "bfill":
            result = result.fillna(method="bfill")
            self.logger.info("Backward-filled nulls")
        else:
            raise TransformerError(f"Unknown fill strategy: {self.fill_null_strategy}")

        # Reset index
        result = result.reset_index(drop=True)

        self.logger.info(
            "Cleaning complete",
            original_rows=len(df),
            final_rows=len(result),
            original_cols=len(df.columns),
            final_cols=len(result.columns),
        )

        return result
