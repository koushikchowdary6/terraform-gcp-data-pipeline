.DEFAULT_GOAL := help
.PHONY: help install fetch run rerun full-refresh quality examples state test lint fmt \
        tf-fmt tf-validate tf-lint tf-scan tf-plan verify clean

PYTHON ?= python3
TERRAFORM ?= terraform

help: ## Show available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install: ## Install Python dependencies (including dev tools)
	$(PYTHON) -m pip install -r requirements-dev.txt

fetch: ## Download the public source feeds into data/raw
	$(PYTHON) -m pipeline.cli fetch

run: ## Run the local pipeline (downloads sources if missing)
	$(PYTHON) -m pipeline.cli run-local

rerun: ## Run again without re-downloading, to show idempotency
	$(PYTHON) -m pipeline.cli run-local --offline

full-refresh: ## Re-process every row, ignoring watermarks
	$(PYTHON) -m pipeline.cli run-local --offline --full-refresh

quality: ## Re-run data-quality checks against the local warehouse
	$(PYTHON) -m pipeline.cli quality

examples: ## Run the example analytical queries
	$(PYTHON) -m pipeline.cli examples

state: ## Show incremental watermarks
	$(PYTHON) -m pipeline.cli state

test: ## Run the test suite
	$(PYTHON) -m pytest -v

lint: ## Lint and check formatting
	$(PYTHON) -m ruff check .
	$(PYTHON) -m ruff format --check .

fmt: ## Auto-fix lint findings and format
	$(PYTHON) -m ruff check --fix .
	$(PYTHON) -m ruff format .

tf-fmt: ## terraform fmt -check
	cd terraform && $(TERRAFORM) fmt -check -recursive -diff

tf-validate: ## terraform init -backend=false && validate
	cd terraform && $(TERRAFORM) init -backend=false -input=false && $(TERRAFORM) validate

tf-lint: ## tflint
	tflint --chdir=terraform

tf-scan: ## checkov security scan
	checkov --directory terraform --framework terraform --compact --quiet

tf-plan: ## terraform plan (requires credentials and terraform.tfvars)
	cd terraform && $(TERRAFORM) plan

verify: lint test tf-fmt ## Everything that runs without cloud credentials

clean: ## Remove generated data and caches (keeps downloaded sources)
	rm -rf data/local data/processed data/rejects .pytest_cache .ruff_cache
	find . -type d -name __pycache__ -prune -exec rm -rf {} +
