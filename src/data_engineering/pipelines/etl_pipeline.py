"""ETL Pipeline orchestrator."""

from typing import Any

import pandas as pd

from data_engineering.extractors.base import BaseExtractor
from data_engineering.loaders.base import BaseLoader
from data_engineering.logger import get_logger
from data_engineering.transformers.base import BaseTransformer


class ETLPipeline:
    """Orchestrates extract, transform, and load operations.

    Provides a fluent interface for building pipelines:

        pipeline = ETLPipeline("daily_sales")
            .extract(CSVExtractor("sales", "data/sales.csv"))
            .transform(CleaningTransformer())
            .load(PostgresLoader(table_name="sales"))

    Then run with:
        pipeline.run()
    """

    def __init__(self, name: str, **kwargs: Any) -> None:
        """Initialize the pipeline.

        Args:
            name: Pipeline identifier.
            **kwargs: Additional configuration.
        """
        self.name = name
        self.config = kwargs
        self.logger = get_logger(f"pipeline.{name}")
        self._extractors: list[BaseExtractor] = []
        self._transformers: list[BaseTransformer] = []
        self._loaders: list[BaseLoader] = []
        self._data: pd.DataFrame | None = None
        self.logger.info(f"Initialized pipeline: {name}")

    def extract(self, extractor: BaseExtractor) -> "ETLPipeline":
        """Add an extractor to the pipeline.

        Args:
            extractor: Extractor instance.

        Returns:
            Self for method chaining.
        """
        self._extractors.append(extractor)
        self.logger.debug(f"Added extractor: {extractor.name}")
        return self

    def transform(self, transformer: BaseTransformer) -> "ETLPipeline":
        """Add a transformer to the pipeline.

        Args:
            transformer: Transformer instance.

        Returns:
            Self for method chaining.
        """
        self._transformers.append(transformer)
        self.logger.debug(f"Added transformer: {transformer.name}")
        return self

    def load(self, loader: BaseLoader) -> "ETLPipeline":
        """Add a loader to the pipeline.

        Args:
            loader: Loader instance.

        Returns:
            Self for method chaining.
        """
        self._loaders.append(loader)
        self.logger.debug(f"Added loader: {loader.name}")
        return self

    def run(self) -> dict[str, Any]:
        """Execute the full pipeline.

        Returns:
            Dictionary with execution metrics.

        Raises:
            PipelineError: If pipeline execution fails.
        """
        self.logger.info(f"Starting pipeline: {self.name}")
        metrics = {
            "pipeline": self.name,
            "extracted_rows": 0,
            "loaded_rows": 0,
            "errors": [],
        }

        try:
            # Extract phase
            self._run_extract(metrics)

            # Transform phase
            self._run_transform(metrics)

            # Load phase
            self._run_load(metrics)

            self.logger.info(
                f"Pipeline {self.name} completed successfully",
                metrics=metrics,
            )
            return metrics

        except Exception as e:
            self.logger.error(
                f"Pipeline {self.name} failed",
                error=str(e),
                metrics=metrics,
            )
            raise PipelineError(f"Pipeline {self.name} failed: {e}") from e

    def _run_extract(self, metrics: dict[str, Any]) -> None:
        """Run extraction phase."""
        if not self._extractors:
            raise PipelineError("No extractors configured")

        dataframes: list[pd.DataFrame] = []

        for extractor in self._extractors:
            self.logger.info(f"Extracting with {extractor.name}")
            try:
                with extractor:
                    df = extractor.extract()
                    dataframes.append(df)
                    metrics["extracted_rows"] += len(df)
            except Exception as e:
                metrics["errors"].append(f"Extractor {extractor.name} failed: {e}")
                raise

        # Combine all extracted data
        if len(dataframes) == 1:
            self._data = dataframes[0]
        else:
            self._data = pd.concat(dataframes, ignore_index=True)
            self.logger.info(
                f"Combined {len(dataframes)} dataframes",
                total_rows=len(self._data),
            )

    def _run_transform(self, metrics: dict[str, Any]) -> None:
        """Run transformation phase."""
        if self._data is None:
            raise PipelineError("No data to transform")

        for transformer in self._transformers:
            self.logger.info(f"Transforming with {transformer.name}")
            try:
                self._data = transformer(self._data)
            except Exception as e:
                metrics["errors"].append(f"Transformer {transformer.name} failed: {e}")
                raise

    def _run_load(self, metrics: dict[str, Any]) -> None:
        """Run loading phase."""
        if self._data is None:
            raise PipelineError("No data to load")

        if not self._loaders:
            self.logger.warning("No loaders configured, data will not be persisted")
            return

        for loader in self._loaders:
            self.logger.info(f"Loading with {loader.name}")
            try:
                with loader:
                    rows = loader(self._data)
                    metrics["loaded_rows"] += rows
            except Exception as e:
                metrics["errors"].append(f"Loader {loader.name} failed: {e}")
                raise

    def preview(self, n: int = 5) -> pd.DataFrame | None:
        """Preview the current data state.

        Args:
            n: Number of rows to preview.

        Returns:
            DataFrame head or None if no data.
        """
        if self._data is not None:
            return self._data.head(n)
        return None


class PipelineError(Exception):
    """Exception raised when pipeline execution fails."""
    pass
