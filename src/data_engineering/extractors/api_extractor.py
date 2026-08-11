"""API data extractor."""

from typing import Any

import httpx
import pandas as pd
from tenacity import retry, stop_after_attempt, wait_exponential

from data_engineering.config import get_settings
from data_engineering.extractors.base import BaseExtractor, ExtractorError
from data_engineering.logger import get_logger


class APIExtractor(BaseExtractor):
    """Extract data from REST APIs."""

    def __init__(
        self,
        name: str,
        endpoint: str,
        method: str = "GET",
        headers: dict[str, str] | None = None,
        params: dict[str, Any] | None = None,
        timeout: int | None = None,
        **kwargs: Any,
    ) -> None:
        """Initialize API extractor.

        Args:
            name: Extractor identifier.
            endpoint: Full URL of the API endpoint.
            method: HTTP method (GET, POST, etc.).
            headers: Additional HTTP headers.
            params: Query parameters or request body.
            timeout: Request timeout in seconds.
            **kwargs: Additional configuration.
        """
        super().__init__(
            name,
            endpoint=endpoint,
            method=method,
            headers=headers,
            params=params,
            timeout=timeout,
            **kwargs,
        )
        self.endpoint = endpoint
        self.method = method.upper()
        self.headers = headers or {}
        self.params = params or {}
        self.timeout = timeout or get_settings().api_timeout
        self.logger = get_logger(f"extractor.api.{name}")

    def validate_source(self) -> bool:
        """Validate API endpoint is accessible."""
        try:
            response = httpx.head(
                self.endpoint,
                headers=self.headers,
                timeout=self.timeout,
            )
            response.raise_for_status()
            return True
        except httpx.HTTPStatusError as e:
            raise ExtractorError(f"API returned error: {e.response.status_code}") from e
        except httpx.RequestError as e:
            raise ExtractorError(f"Cannot connect to API: {e}") from e

    @retry(
        stop=stop_after_attempt(get_settings().max_retries),
        wait=wait_exponential(
            multiplier=get_settings().retry_delay,
            exp_base=get_settings().retry_backoff,
        ),
        reraise=True,
    )
    def extract(self) -> pd.DataFrame:
        """Fetch data from API and convert to DataFrame.

        Returns:
            DataFrame with API response data.

        Raises:
            ExtractorError: If API request fails.
        """
        try:
            self.logger.info(
                "Making API request",
                endpoint=self.endpoint,
                method=self.method,
            )

            with httpx.Client(timeout=self.timeout) as client:
                response = client.request(
                    method=self.method,
                    url=self.endpoint,
                    headers=self.headers,
                    params=self.params if self.method == "GET" else None,
                    json=self.params if self.method != "GET" else None,
                )
                response.raise_for_status()

            data = response.json()

            # Handle different response structures
            if isinstance(data, list):
                df = pd.DataFrame(data)
            elif isinstance(data, dict) and "data" in data:
                df = pd.DataFrame(data["data"])
            elif isinstance(data, dict):
                df = pd.DataFrame([data])
            else:
                raise ExtractorError(f"Unexpected API response format: {type(data)}")

            self.logger.info(
                "Successfully fetched API data",
                rows=len(df),
                columns=list(df.columns),
            )
            return df

        except httpx.HTTPStatusError as e:
            raise ExtractorError(
                f"API request failed with status {e.response.status_code}"
            ) from e
        except httpx.RequestError as e:
            raise ExtractorError(f"API request failed: {e}") from e
        except ValueError as e:
            raise ExtractorError(f"Failed to parse API response: {e}") from e
