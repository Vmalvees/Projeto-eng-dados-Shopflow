.PHONY: setup test test-unit lint format generate-data run-pipeline docker-up docker-down clean help

setup:
	pip install -r requirements-dev.txt
	pre-commit install

test:
	pytest tests/ -v --cov=src --cov-report=term-missing --cov-report=html

test-unit:
	pytest tests/unit/ -v

lint:
	ruff check src/ tests/
	mypy src/

format:
	ruff format src/ tests/

generate-data:
	python -m src.extract.data_generator --volume 1000

run-pipeline:
	python -m src.main --local-only

docker-up:
	docker compose -f infrastructure/docker-compose.yml up -d

docker-down:
	docker compose -f infrastructure/docker-compose.yml down

clean:
	rm -rf __pycache__ .pytest_cache .ruff_cache .mypy_cache htmlcov .coverage
	find . -type d -name "__pycache__" -exec rm -rf {} +

help:
	@echo "Available commands:"
	@echo "  setup         : Install dev dependencies and pre-commit hooks"
	@echo "  test          : Run all tests with coverage report"
	@echo "  test-unit     : Run only unit tests"
	@echo "  lint          : Run ruff check and mypy"
	@echo "  format        : Format code using ruff format"
	@echo "  generate-data : Generate mock e-commerce data"
	@echo "  run-pipeline  : Run the ETL pipeline locally"
	@echo "  docker-up     : Start Airflow, Postgres, and MinIO containers"
	@echo "  docker-down   : Stop docker containers"
	@echo "  clean         : Remove cache files and build artifacts"
