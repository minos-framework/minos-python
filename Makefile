.PHONY: install docs

DOCS_TARGET := ./docs/api-reference/packages

install:
	poetry install

docs:
	rm -rf docs/api-reference
	poetry run $(MAKE) --directory=docs html
