"""Spark session management for local development."""
import os

from pyspark.sql import SparkSession

from data_engineering.logger import get_logger

logger = get_logger("spark.session")
_SPARK_SESSION = None

SNOWFLAKE_PACKAGES = os.getenv(
    "SPARK_SNOWFLAKE_PACKAGES",
    "net.snowflake:spark-snowflake_2.13:3.1.1,net.snowflake:snowflake-jdbc:3.20.0",
)


def get_spark_session(
    app_name: str = "DataEngineering",
    master: str = "local[*]",
    memory: str | None = None,
) -> SparkSession:
    """Get or create a Spark session."""
    global _SPARK_SESSION

    if (
        _SPARK_SESSION is not None
        and not _SPARK_SESSION.sparkContext._jsc.sc().isStopped()
    ):
        return _SPARK_SESSION

    driver_mem = memory or os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_mem = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    cores = os.getenv("SPARK_EXECUTOR_CORES", "4")
    partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")

    logger.info(
        "Creating Spark session",
        app_name=app_name,
        master=master,
        driver_memory=driver_mem,
        executor_memory=executor_mem,
        cores=cores,
        partitions=partitions,
        packages=SNOWFLAKE_PACKAGES,
    )

    _SPARK_SESSION = (
        SparkSession.builder
        .appName(app_name)
        .master(master)
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "localhost")
        .config("spark.jars.packages", SNOWFLAKE_PACKAGES)
        .config("spark.driver.memory", driver_mem)
        .config("spark.executor.memory", executor_mem)
        .config("spark.executor.cores", cores)
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
        .config("spark.sql.shuffle.partitions", partitions)
        .config("spark.default.parallelism", partitions)
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.maxRecordsPerBatch", "10000")
        .config("spark.sql.files.maxPartitionBytes", "128m")
        .config("spark.local.dir", "D:/tmp/spark-temp")
        .config("spark.log.level", "WARN")
        .getOrCreate()
    )

    sc = _SPARK_SESSION.sparkContext
    logger.info(
        "Spark session created",
        version=_SPARK_SESSION.version,
        ui_url=sc.uiWebUrl,
        app_id=sc.applicationId,
        default_parallelism=sc.defaultParallelism,
    )

    return _SPARK_SESSION


def stop_spark_session() -> None:
    """Stop the global Spark session."""
    global _SPARK_SESSION
    if _SPARK_SESSION is not None:
        logger.info("Stopping Spark session")
        _SPARK_SESSION.stop()
        _SPARK_SESSION = None
