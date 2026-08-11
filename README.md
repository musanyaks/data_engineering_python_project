# Data Engineering Pipeline

Production-ready Python framework for ETL/ELT data pipelines with **Apache Airflow**, **dbt**, **Snowflake**, and **Apache Spark**.

## Hardware Requirements

This project is **optimized for 16GB RAM / 100GB SSD laptops**.

| Service | Memory | Cores | Notes |
|---------|--------|-------|-------|
| Spark Driver | 4GB | - | Job coordination |
| Spark Executor | 4GB | 4 | Data processing |
| Airflow (all) | ~4GB | - | Webserver + Scheduler + Worker |
| PostgreSQL | 1GB | - | Metadata + staging |
| Redis | 512MB | - | Celery broker |
| OS + Buffer | ~2.5GB | - | Headroom for OS |
| **Total** | **~16GB** | **4+** | Comfortable fit |

## Quick Start

### Option 1: Local Python (No Docker) - Lightest

Best for development and testing individual components.

```bash
# Install everything
make install-dev

# Run a Spark job locally (uses 4GB RAM)
python -m spark.jobs.process_sales

# Run tests
make test
```

### Option 2: Docker Spark Only - Medium

Best for testing Spark cluster behavior.

```bash
# Start Spark master + worker (~8GB RAM)
make spark-up

# Submit jobs
make spark-submit JOB=spark/jobs/process_sales.py

# Open PySpark shell
make spark-shell

# Check status
make spark-status

# Monitor resources
make resource-check
```

### Option 3: Full Stack - Heavy

Best for end-to-end integration testing.

```bash
# Start everything: Airflow + Spark + PostgreSQL + Redis (~12-14GB RAM)
make up

# Access services:
# Airflow UI:  http://localhost:8080  (airflow/airflow)
# Spark UI:    http://localhost:8081
# pgAdmin:     http://localhost:5050  (admin@example.com/admin)

# Monitor Docker resource usage
make resource-check

# Stop everything
make down
```

## Project Structure

```
├── airflow/
│   ├── dags/
│   │   ├── etl_pipeline_dag.py      # Pandas-based ETL
│   │   ├── spark_etl_dag.py         # Spark-based ETL
│   │   └── dbt_daily_dag.py         # Daily dbt run
│   ├── plugins/
│   └── config/
├── dbt/
│   ├── models/
│   │   ├── staging/                 # Staging models (views)
│   │   ├── intermediate/            # Intermediate transformations
│   │   └── marts/                   # Business logic models
│   ├── tests/                       # Custom data tests
│   ├── macros/                      # Reusable SQL macros
│   ├── snapshots/                   # Slowly changing dimensions
│   ├── seeds/                       # Static CSV data
│   ├── dbt_project.yml
│   └── profiles.yml
├── spark/
│   ├── jobs/                        # PySpark job scripts
│   │   ├── process_sales.py         # CSV → PostgreSQL
│   │   └── snowflake_to_postgres.py # Snowflake → PostgreSQL
│   └── config/                      # Spark configuration
│       ├── spark-defaults.conf      # 16GB-optimized settings
│       └── log4j.properties         # Reduced log noise
├── src/data_engineering/
│   ├── extractors/
│   │   ├── csv_extractor.py         # Pandas CSV
│   │   ├── api_extractor.py         # Pandas API
│   │   └── snowflake/
│   │       └── snowflake_extractor.py
│   ├── transformers/
│   │   └── cleaning_transformer.py  # Pandas
│   ├── loaders/
│   │   ├── postgres_loader.py       # Pandas
│   │   └── snowflake/
│   │       └── snowflake_loader.py
│   ├── spark/                       # PySpark components
│   │   ├── spark_session.py         # Spark session manager
│   │   ├── extractors.py            # Spark CSV/Snowflake
│   │   ├── transformers.py          # Spark cleaning
│   │   └── loaders.py               # Spark PostgreSQL/Snowflake
│   ├── pipelines/
│   │   └── etl_pipeline.py          # Pandas pipeline
│   ├── integrations/
│   │   ├── dbt_runner.py            # Programmatic dbt
│   │   └── airflow_utils.py         # Airflow helpers
│   └── utils/
│       └── validators.py
├── tests/
│   ├── unit/
│   └── integration/
├── scripts/
│   ├── run_pipeline.py
│   └── dbt/
│       └── run_dbt.py
├── docker-compose.yml               # 16GB RAM resource limits
├── Dockerfile.airflow
├── Dockerfile.dbt
├── Dockerfile.spark
├── Makefile                         # Resource-aware commands
├── pyproject.toml
└── README.md
```

## Spark Usage

### Local Mode (Single Machine)

```python
from data_engineering.spark.spark_session import get_spark_session
from data_engineering.spark.extractors import SparkCSVExtractor
from data_engineering.spark.transformers import SparkCleaningTransformer
from data_engineering.spark.loaders import SparkPostgresLoader

# Uses 4GB driver, all CPU cores, 8 shuffle partitions
spark = get_spark_session("my_job")

# For bigger jobs, increase memory:
spark = get_spark_session("big_job", memory="6g")

# Extract
df = SparkCSVExtractor("sales", "data/sales.csv").extract()

# Transform
clean_df = SparkCleaningTransformer("clean").transform(df)

# Load
SparkPostgresLoader("pg", table_name="staging_sales").load(clean_df)
```

### Docker Cluster Mode

```bash
# Spark master + worker run in containers
# Submit jobs via Airflow or directly:

make spark-submit JOB=spark/jobs/process_sales.py
```

### Spark Configuration for 16GB RAM

| Setting | Value | Reason |
|---------|-------|--------|
| Driver Memory | 4GB | Enough for aggregations and UI |
| Executor Memory | 4GB | Parallel processing without OOM |
| Executor Cores | 4 | Matches typical laptop CPUs |
| Shuffle Partitions | 8 | Reduces small-file overhead |
| AQE | Enabled | Auto-optimizes joins at runtime |
| Local Dir | /tmp/spark-temp | Uses fast SSD for spills |

### Tuning for Your Data Size

**Small data (< 1GB):**
```bash
# Use pandas instead - faster startup, less overhead
python -m scripts.run_pipeline
```

**Medium data (1-10GB):**
```bash
# Default Spark settings work well
python -m spark.jobs.process_sales
```

**Large data (10-50GB):**
```bash
# Increase memory, use Docker cluster
export SPARK_DRIVER_MEMORY=6g
export SPARK_EXECUTOR_MEMORY=6g
make spark-up
make spark-submit JOB=spark/jobs/process_sales.py
```

## dbt Models

### Staging Layer
- `stg_sales` - Cleaned sales transactions
- `stg_customers` - Standardized customer data
- `stg_products` - Product catalog

### Intermediate Layer
- `int_sales_enriched` - Sales joined with customers and products

### Marts Layer
- `fct_sales` - Incremental fact table
- `dim_customers` - Customer dimension with metrics
- `dim_products` - Product dimension with sales stats
- `fct_daily_revenue` - Daily aggregated revenue

## Snowflake Integration

### Authentication
Supports both password and key-pair authentication:

```python
# Password auth
extractor = SnowflakeExtractor(
    name="sf_query",
    query="SELECT * FROM raw_events",
    account="xy12345.us-east-1",
    user="myuser",
    password="mypassword",
)

# Key-pair auth
extractor = SnowflakeExtractor(
    name="sf_query",
    query="SELECT * FROM raw_events",
    private_key_path="/path/to/key.p8",
)
```

### Environment Variables
All Snowflake connection parameters can be set via environment variables:
- `SNOWFLAKE_ACCOUNT`
- `SNOWFLAKE_USER`
- `SNOWFLAKE_PASSWORD`
- `SNOWFLAKE_PRIVATE_KEY_PATH`
- `SNOWFLAKE_ROLE`
- `SNOWFLAKE_DATABASE`
- `SNOWFLAKE_WAREHOUSE`
- `SNOWFLAKE_SCHEMA`

## Airflow DAGs

### `etl_master_pipeline`
Hourly pipeline using **pandas** for smaller datasets:
1. Extracts CSV data to PostgreSQL staging
2. Extracts Snowflake data to staging
3. Runs data quality checks
4. Executes dbt models
5. Runs dbt tests
6. Loads results to Snowflake marts

### `spark_etl_pipeline`
Hourly pipeline using **Spark** for larger datasets:
1. Validates input files
2. Submits Spark job: CSV → PostgreSQL
3. Submits Spark job: Snowflake → PostgreSQL
4. Runs dbt models
5. Runs dbt tests

### `dbt_daily_transformations`
Daily batch job:
1. Installs dbt dependencies
2. Loads seeds
3. Runs all models
4. Executes tests
5. Generates documentation
6. Runs snapshots

## Testing

```bash
# Unit tests only
pytest -m unit

# Integration tests
pytest -m integration

# Spark-specific tests
pytest -m spark

# Snowflake-specific tests
pytest -m snowflake

# All tests with coverage
pytest
```

## Key Production Features

| Feature | Implementation |
|---------|---------------|
| **Orchestration** | Apache Airflow with CeleryExecutor |
| **Distributed Processing** | Apache Spark (local or cluster) |
| **Transformations** | dbt with incremental models |
| **Data Warehouse** | Snowflake with connection pooling |
| **Authentication** | Password or key-pair auth |
| **Logging** | Structured JSON logs via structlog |
| **Retries** | Exponential backoff with tenacity |
| **Testing** | pytest + dbt tests + data quality checks |
| **CI/CD** | GitHub Actions with matrix testing |
| **Containerization** | Docker Compose with resource limits |
| **Laptop Optimized** | 16GB RAM tuned, 4GB Spark heaps |

## Makefile Commands

| Command | Description | RAM Usage |
|---------|-------------|-----------|
| `make up` | Start all services | ~12-14GB |
| `make down` | Stop all services | - |
| `make spark-up` | Start Spark only | ~8GB |
| `make spark-submit JOB=...` | Submit Spark job | +0GB |
| `make spark-shell` | PySpark shell | +0GB |
| `make airflow-up` | Start Airflow only | ~4GB |
| `make dbt-run` | Run dbt models | Minimal |
| `make test` | Run Python tests | ~1GB |
| `make resource-check` | Show Docker usage | - |

## Troubleshooting

### Out of Memory Errors

If Spark fails with OOM:
```bash
# Reduce Spark memory
export SPARK_DRIVER_MEMORY=2g
export SPARK_EXECUTOR_MEMORY=2g

# Or use pandas for smaller datasets
python -m scripts.run_pipeline
```

### Docker Disk Space (100GB SSD)

If running low on disk:
```bash
# Clean up Docker
make clean
docker system prune -a

# Check Spark temp files
rm -rf /tmp/spark-temp/*
```

### Slow Performance

1. **Check partitions**: Too many = overhead, too few = not parallel
2. **Use AQE**: Already enabled, but verify `spark.sql.adaptive.enabled=true`
3. **SSD for spills**: Already configured to use `/tmp/spark-temp`
4. **Reduce shuffle**: Use `.coalesce()` instead of `.repartition()` when reducing
