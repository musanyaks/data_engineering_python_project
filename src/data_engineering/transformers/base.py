"""Base transformer interface."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from data_engineering.logger import get_logger


class BaseTransformer(ABC):
    """Abstract base class for all data transformers.

    Transformers take a DataFrame, apply transformations, and return
    a modified DataFrame.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Initialize the transformer.

        Args:
            name: Unique identifier for this transformer.
            **kwargs: Additional configuration options.
        """
        self.name = name
        self.config = kwargs
        self.logger = get_logger(f"transformer.{name}")
        self.logger.info(f"Initialized transformer: {name}")

    @abstractmethod
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transform the input DataFrame.

        Args:
            df: Input DataFrame to transform.

        Returns:
            Transformed DataFrame.

        Raises:
            TransformerError: If transformation fails.
        """
        ...

    def validate_input(self, df: pd.DataFrame) -> bool:
        """Validate input DataFrame before transformation.

        Args:
            df: Input DataFrame to validate.

        Returns:
            True if input is valid.

        Raises:
            TransformerError: If input validation fails.
        """
        if df is None:
            raise TransformerError("Input DataFrame is None")
        if df.empty:
            self.logger.warning("Input DataFrame is empty")
        return True

    def __call__(self, df: pd.DataFrame) -> pd.DataFrame:
        """Allow transformer to be called as a function.

        Args:
            df: Input DataFrame.

        Returns:
            Transformed DataFrame.
        """
        self.validate_input(df)
        result = self.transform(df)
        self.logger.info(
            f"Transformation {self.name} complete",
            input_rows=len(df),
            output_rows=len(result),
        )
        return result


class TransformerError(Exception):
    """Exception raised when data transformation fails."""
    pass
