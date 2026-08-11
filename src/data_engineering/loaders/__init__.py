"""Data loaders for various destinations."""

from data_engineering.loaders.base import BaseLoader
from data_engineering.loaders.postgres_loader import PostgresLoader

__all__ = ["BaseLoader", "PostgresLoader"]
