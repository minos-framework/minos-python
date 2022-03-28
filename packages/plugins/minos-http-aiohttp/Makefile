.PHONY: docs

lint:
	poetry run flake8

test:
	poetry run pytest

coverage:
	poetry run coverage run -m pytest
	poetry run coverage report -m
	poetry run coverage xml

reformat:
	poetry run black --line-length 120 minos tests
	poetry run isort minos tests

docs:
	rm -rf docs/api
	poetry run $(MAKE) --directory=docs html

release:
	$(MAKE) dist
	poetry publish

dist:
	poetry build
	ls -l dist

install:
	poetry install

update:
	poetry update

check:
	$(MAKE) install
	$(MAKE) reformat
	$(MAKE) lint
	$(MAKE) test
	$(MAKE) docs
	$(MAKE) dist
