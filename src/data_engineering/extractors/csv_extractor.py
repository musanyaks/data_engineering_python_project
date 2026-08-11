"""CSV file extractor."""

from pathlib import Path
from typing import Any

import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.extractors.base import BaseExtractor, ExtractorError
from data_engineering.logger import get_logger


class CSVExtractor(BaseExtractor):
    """Extract data from CSV files."""

    def __init__(
        self,
        name: str,
        file_path: str | Path,
        **pandas_kwargs: Any,
    ) -> None:
        """Initialize CSV extractor.

        Args:
            name: Extractor identifier.
            file_path: Path to the CSV file.
            **pandas_kwargs: Additional arguments passed to pd.read_csv().
        """
        super().__init__(name, file_path=file_path, **pandas_kwargs)
        self.file_path = Path(file_path)
        self.pandas_kwargs = pandas_kwargs
        self.logger = get_logger(f"extractor.csv.{name}")

    def validate_source(self) -> bool:
        """Check if CSV file exists and is readable."""
        if not self.file_path.exists():
            raise ExtractorError(f"CSV file not found: {self.file_path}")
        if not self.file_path.is_file():
            raise ExtractorError(f"Path is not a file: {self.file_path}")
        return True

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def extract(self) -> pd.DataFrame:
        """Read CSV file into DataFrame.

        Returns:
            DataFrame with CSV data.

        Raises:
            ExtractorError: If file cannot be read.
        """
        try:
            self.logger.info(
                "Reading CSV file",
                file_path=str(self.file_path),
                kwargs=self.pandas_kwargs,
            )

            df = pd.read_csv(
                self.file_path,
                **self.pandas_kwargs,
            )

            self.logger.info(
                "Successfully read CSV",
                rows=len(df),
                columns=list(df.columns),
            )
            return df

        except pd.errors.EmptyDataError as e:
            raise ExtractorError(f"CSV file is empty: {self.file_path}") from e
        except pd.errors.ParserError as e:
            raise ExtractorError(f"Failed to parse CSV: {self.file_path}") from e
        except Exception as e:
            raise ExtractorError(f"Unexpected error reading CSV: {e}") from e
