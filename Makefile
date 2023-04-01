#* Variables
SHELL := /usr/bin/env bash
PYTHON := python
PYTHONPATH := `pwd`


#* Poetry
.PHONY: poetry-download
poetry-download:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | $(PYTHON) -

.PHONY: poetry-remove
poetry-remove:
	curl -sSL https://raw.githubusercontent.com/python-poetry/poetry/master/install-poetry.py | $(PYTHON) - --uninstall

#* Installation
.PHONY: install
install:
	poetry lock -n && poetry export --without-hashes > requirements.txt
	poetry install -n
	-poetry run pyright

.PHONY: install-pre-commit
install-pre-commit:
	poetry run pre-commit install

#* Formatters
.PHONY: codestyle
codestyle:
	poetry run pyupgrade --exit-zero-even-if-changed --py38-plus src/**/*.py
	poetry run isort --settings-path pyproject.toml ./
	poetry run black --config pyproject.toml ./

.PHONY: format
format: codestyle

#* Linting
.PHONY: test
test:
	PYTHONPATH=$(PYTHONPATH) poetry run pytest -c pyproject.toml --cov-report=html --cov=src tests/
	poetry run coverage-badge -o assets/images/coverage.svg -f

.PHONY: check-codestyle
check-codestyle:
	poetry run isort --diff --check-only --settings-path pyproject.toml ./
	poetry run black --diff --check --config pyproject.toml ./
	poetry run flake8 -vvv --toml-config pyproject.toml

.PHONY: pyright
pyright:
	poetry run pyright

.PHONY: check-safety
check-safety:
	poetry check
	poetry run safety check --full-report
	poetry run bandit -ll --recursive src tests

.PHONY: lint
lint: test check-codestyle pyright check-safety

.PHONY: update-dev-deps
update-dev-deps:
	poetry add --group dev --allow-prereleases black@latest
	poetry add --group dev --extras colors isort@latest
	poetry add --group dev \
						bandit@latest \
						darglint@latest \
						flake8@latest \
						flake8-pyproject@latest \
						pre-commit@latest \
						pydocstyle@latest \
						pylint@latest \
						pyright@latest \
						pytest@latest \
						pyupgrade@latest \
						safety@latest \
						coverage@latest \
						coverage-badge@latest \
						pytest-html@latest \
						pytest-cov@latest \
						;

#* Cleaning
.PHONY: pycache-remove
pycache-remove:
	find . | grep -E "(__pycache__|\.pyc|\.pyo$$)" | xargs rm -rf

.PHONY: dsstore-remove
dsstore-remove:
	find . | grep -E ".DS_Store" | xargs rm -rf

.PHONY: ipynbcheckpoints-remove
ipynbcheckpoints-remove:
	find . | grep -E ".ipynb_checkpoints" | xargs rm -rf

.PHONY: pytestcache-remove
pytestcache-remove:
	find . | grep -E ".pytest_cache" | xargs rm -rf
	find . | grep -E ".coverage" | xargs rm -rf
	find . | grep -E "htmlcov" | xargs rm -rf

.PHONY: build-remove
build-remove:
	rm -rf build/

.PHONY: clean
clean: pycache-remove dsstore-remove ipynbcheckpoints-remove pytestcache-remove
