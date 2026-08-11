"""Base loader interface."""

from abc import ABC, abstractmethod
from typing import Any

import pandas as pd

from data_engineering.logger import get_logger


class BaseLoader(ABC):
    """Abstract base class for all data loaders.

    Loaders take a DataFrame and persist it to a destination.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Initialize the loader.

        Args:
            name: Unique identifier for this loader.
            **kwargs: Additional configuration options.
        """
        self.name = name
        self.config = kwargs
        self.logger = get_logger(f"loader.{name}")
        self.logger.info(f"Initialized loader: {name}")

    @abstractmethod
    def load(self, df: pd.DataFrame) -> int:
        """Load data to the destination.

        Args:
            df: DataFrame to load.

        Returns:
            Number of rows loaded.

        Raises:
            LoaderError: If loading fails.
        """
        ...

    def validate_input(self, df: pd.DataFrame) -> bool:
        """Validate input DataFrame before loading.

        Args:
            df: Input DataFrame to validate.

        Returns:
            True if input is valid.

        Raises:
            LoaderError: If input validation fails.
        """
        if df is None:
            raise LoaderError("Input DataFrame is None")
        if df.empty:
            self.logger.warning("Input DataFrame is empty, nothing to load")
        return True

    def __call__(self, df: pd.DataFrame) -> int:
        """Allow loader to be called as a function.

        Args:
            df: Input DataFrame.

        Returns:
            Number of rows loaded.
        """
        self.validate_input(df)
        rows = self.load(df)
        self.logger.info(
            f"Loaded {rows} rows using {self.name}",
            rows=rows,
            loader=self.name,
        )
        return rows


class LoaderError(Exception):
    """Exception raised when data loading fails."""
    pass
