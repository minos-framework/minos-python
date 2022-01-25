.PHONY: clean clean-test clean-pyc clean-build docs help
.DEFAULT_GOAL := help

define BROWSER_PYSCRIPT
import os, webbrowser, sys

from urllib.request import pathname2url

webbrowser.open("file://" + pathname2url(os.path.abspath(sys.argv[1])))
endef
export BROWSER_PYSCRIPT

define PRINT_HELP_PYSCRIPT
import re, sys

for line in sys.stdin:
	match = re.match(r'^([a-zA-Z_-]+):.*?## (.*)$$', line)
	if match:
		target, help = match.groups()
		print("%-20s %s" % (target, help))
endef
export PRINT_HELP_PYSCRIPT

BROWSER := python -c "$$BROWSER_PYSCRIPT"

help:
	@python -c "$$PRINT_HELP_PYSCRIPT" < $(MAKEFILE_LIST)

clean: clean-build clean-pyc clean-test clean-env

clean-build: ## remove build artifacts
	rm -fr build/
	rm -fr dist/
	rm -fr .eggs/
	find . -name '*.egg-info' -exec rm -fr {} +
	find . -name '*.egg' -exec rm -f {} +

clean-pyc: ## remove Python file artifacts
	find . -name '*.pyc' -exec rm -f {} +
	find . -name '*.pyo' -exec rm -f {} +
	find . -name '*~' -exec rm -f {} +
	find . -name '__pycache__' -exec rm -fr {} +

clean-test: ## remove test and coverage artifacts
	rm -fr .tox/
	rm -f .coverage
	rm -fr htmlcov/
	rm -fr .pytest_cache

clean-env:
	rm -fr .env/

lint: ## check style with flake8
	poetry run flake8

test: ## run tests quickly with the default Python
	poetry run pytest

test-all: ## run tests on every Python version with tox
	poetry run tox

coverage: ## check code coverage quickly with the default Python
	poetry run coverage run -m pytest
	poetry run coverage report -m
	poetry run coverage xml
	## $(BROWSER) htmlcov/index.html

reformat: ## check code coverage quickly with the default Python
	poetry run black --line-length 120 minos tests
	poetry run isort minos tests

docs: ## generate Sphinx HTML documentation, including API docs
	rm -rf docs/api
	poetry run $(MAKE) -C docs clean html

servedocs: docs ## compile the docs watching for changes
	watchmedo shell-command -p '*.rst' -c '$(MAKE) -C docs html' -R -D .

release: dist ## package and upload a release
	poetry publish

dist: clean ## builds source and wheel package
	poetry build
	ls -l dist

install:
	poetry install

update:
	poetry update

full-install: clean install
