"""Base extractor interface."""

from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar

import pandas as pd

from data_engineering.logger import get_logger

T = TypeVar("T")


class BaseExtractor(ABC, Generic[T]):
    """Abstract base class for all data extractors.

    Provides common functionality for extracting data from various sources.
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Initialize the extractor.

        Args:
            name: Unique identifier for this extractor.
            **kwargs: Additional configuration options.
        """
        self.name = name
        self.config = kwargs
        self.logger = get_logger(f"extractor.{name}")
        self.logger.info(f"Initialized extractor: {name}")

    @abstractmethod
    def extract(self) -> pd.DataFrame:
        """Extract data from the source.

        Returns:
            DataFrame containing the extracted data.

        Raises:
            ExtractorError: If extraction fails.
        """
        ...

    def validate_source(self) -> bool:
        """Validate that the data source is accessible.

        Returns:
            True if source is valid and accessible.

        Raises:
            ExtractorError: If source validation fails.
        """
        return True

    def __enter__(self) -> "BaseExtractor[T]":
        """Context manager entry."""
        self.validate_source()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """Context manager exit."""
        if exc_type is not None:
            self.logger.error(
                f"Extractor {self.name} failed",
                exc_info=(exc_type, exc_val, exc_tb),
            )


class ExtractorError(Exception):
    """Exception raised when data extraction fails."""
    pass
