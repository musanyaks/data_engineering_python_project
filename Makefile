.PHONY: install test lint format clean docker-build docker-run airflow-init airflow-up airflow-down dbt-deps dbt-run dbt-test spark-up spark-down spark-submit spark-shell spark-status help resource-check

PYTHON := python3
PIP := pip3

help:
	@echo "Data Engineering Pipeline - 16GB RAM Optimized"
	@echo ""
	@echo "Setup:"
	@echo "  install        Install dependencies"
	@echo "  install-dev    Install dev + airflow + dbt + spark deps"
	@echo ""
	@echo "Development:"
	@echo "  test           Run tests"
	@echo "  lint           Run linters"
	@echo "  format         Format code"
	@echo "  clean          Clean build artifacts"
	@echo ""
	@echo "Docker Services:"
	@echo "  up             Start ALL services (uses ~12GB RAM)"
	@echo "  down           Stop all services"
	@echo "  spark-up       Start Spark only (uses ~8GB RAM)"
	@echo "  spark-down     Stop Spark"
	@echo "  airflow-up     Start Airflow only (uses ~4GB RAM)"
	@echo "  airflow-down   Stop Airflow"
	@echo ""
	@echo "Spark Jobs:"
	@echo "  spark-submit   Submit a Spark job"
	@echo "  spark-shell    Open PySpark shell"
	@echo "  spark-status   Show Spark UI URL"
	@echo ""
	@echo "dbt:"
	@echo "  dbt-deps       Install dbt packages"
	@echo "  dbt-run        Run dbt models"
	@echo "  dbt-test       Run dbt tests"
	@echo "  dbt-docs       Generate and serve docs"
	@echo ""
	@echo "Monitoring:"
	@echo "  resource-check Show Docker resource usage"

install:
	$(PIP) install -e .

install-dev:
	$(PIP) install -e ".[dev,airflow,dbt,spark]"
	pre-commit install

test:
	pytest

lint:
	ruff check src tests
	mypy src
	black --check src tests

format:
	black src tests
	ruff check --fix src tests

clean:
	rm -rf build/ dist/ *.egg-info/ .pytest_cache/ .mypy_cache/ htmlcov/ .coverage
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete
	-docker volume rm data_engineering_project_spark_tmp 2>/dev/null || true

resource-check:
	@echo "=== Docker Resource Usage ==="
	@docker system df
	@echo ""
	@echo "=== Container Memory ==="
	@docker stats --no-stream --format "table {{.Name}}\t{{.MemUsage}}\t{{.MemPerc}}" 2>/dev/null || echo "No containers running"

airflow-init:
	docker-compose up airflow-init

airflow-up:
	docker-compose up -d postgres redis airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
	@echo "Airflow UI: http://localhost:8090 (airflow/airflow)"

airflow-down:
	docker-compose stop airflow-webserver airflow-scheduler airflow-worker airflow-triggerer

spark-up:
	docker-compose up -d spark-master spark-worker
	@echo "Spark UI: http://localhost:8082"
	@echo "Spark Master: spark://localhost:7077"

spark-down:
	docker-compose stop spark-master spark-worker

spark-submit:
	@echo "Usage: make spark-submit JOB=spark/jobs/process_sales.py"
	docker-compose exec spark-master spark-submit \
		--master spark://spark-master:7077 \
		--executor-memory 4g \
		--driver-memory 4g \
		$(JOB)

spark-shell:
	docker-compose exec spark-master pyspark --master spark://spark-master:7077

spark-status:
	@echo "Spark UI: http://localhost:8082"
	@docker-compose ps spark-master spark-worker

dbt-deps:
	dbt deps --project-dir dbt --profiles-dir dbt

dbt-run:
	dbt run --project-dir dbt --profiles-dir dbt

dbt-test:
	dbt test --project-dir dbt --profiles-dir dbt

dbt-docs:
	dbt docs generate --project-dir dbt --profiles-dir dbt
	dbt docs serve --project-dir dbt --profiles-dir dbt

# Start everything - requires 16GB RAM
up:
	@echo "Starting full stack..."
	@echo "Recommended: 16GB RAM, 100GB+ SSD"
	@echo ""
	docker-compose up -d postgres redis spark-master spark-worker
	@echo "Waiting for infrastructure..."
	sleep 10
	docker-compose up -d airflow-init
	@echo "Waiting for Airflow init..."
	sleep 15
	docker-compose up -d airflow-webserver airflow-scheduler airflow-worker airflow-triggerer
	@echo ""
	@echo "All services started!"
	@echo "Airflow UI:  http://localhost:8090"
	@echo "Spark UI:    http://localhost:8082"
	@echo "pgAdmin:     http://localhost:5050"
	@echo ""
	@echo "Run 'make resource-check' to monitor usage"

down:
	docker-compose down

# Quick start for local Python development (no Docker)
run-spark-local:
	python -m spark.jobs.process_sales

run-spark-sf:
	python -m spark.jobs.snowflake_to_postgres
