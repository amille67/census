.PHONY: help install dev test lint spine ingest assemble clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

install:  ## Install base dependencies
	pip install -r requirements/base.txt

dev:  ## Install dev dependencies
	pip install -r requirements/dev.txt

test:  ## Run tests
	pytest tests/ -v --tb=short

lint:  ## Run linters
	flake8 backend/ tests/
	black --check backend/ tests/
	isort --check backend/ tests/

spine:  ## Build master spine crosswalk
	python -m backend.scripts.build_master_spine

ingest:  ## Run universal ingest (pass SOURCE= and SCENARIO=)
	python -m backend.scripts.universal_ingest --source $(SOURCE) --scenario $(SCENARIO)

assemble:  ## Assemble master blockgroup table
	python -m backend.scripts.assemble_master_blockgroup

clean:  ## Remove staging and temp files
	rm -rf data/staging/*/temp_* data/staging/*_tmp*
	find data/staging -name "*_tmp.*" -delete
