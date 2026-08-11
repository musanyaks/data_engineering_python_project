"""Spark job to move data from Snowflake to PostgreSQL.

Useful for replicating Snowflake data to local PostgreSQL for development.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "src"))

from data_engineering.logger import configure_logging, get_logger
from data_engineering.spark.extractors import SparkSnowflakeExtractor
from data_engineering.spark.loaders import SparkPostgresLoader
from data_engineering.spark.spark_session import get_spark_session, stop_spark_session


def main() -> int:
    """Replicate Snowflake table to PostgreSQL."""
    configure_logging()
    logger = get_logger("spark.job.replicate")

    try:
        spark = get_spark_session("replicate", memory="2g")

        # Extract from Snowflake
        logger.info("Reading from Snowflake")
        extractor = SparkSnowflakeExtractor(
            "sf",
            query="SELECT * FROM analytics.raw.kafka_events LIMIT 10000",
        )
        df = extractor.extract()

        # Load to PostgreSQL
        logger.info("Writing to PostgreSQL")
        loader = SparkPostgresLoader(
            "pg",
            table_name="raw_events",
            mode="overwrite",
        )
        rows = loader.load(df)

        logger.info(f"Replicated {rows} rows")
        return 0

    except Exception as e:
        logger.error(f"Replication failed: {e}")
        return 1
    finally:
        stop_spark_session()


if __name__ == "__main__":
    sys.exit(main())
