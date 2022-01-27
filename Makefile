.PHONY: install docs

DOCS_TARGET := ./docs/api-reference/packages

install:
	poetry install

docs:
	mkdir -p $(DOCS_TARGET)/core

	$(MAKE) --directory=packages/core/minos-microservice-aggregate install docs
	cp -R packages/core/minos-microservice-aggregate/docs/_build/html $(DOCS_TARGET)/core/minos-microservice-aggregate

	$(MAKE) --directory=packages/core/minos-microservice-common install docs
	cp -R packages/core/minos-microservice-common/docs/_build/html $(DOCS_TARGET)/core/minos-microservice-common

	$(MAKE) --directory=packages/core/minos-microservice-cqrs install docs
	cp -R packages/core/minos-microservice-cqrs/docs/_build/html $(DOCS_TARGET)/core/minos-microservice-cqrs

	$(MAKE) --directory=packages/core/minos-microservice-networks install docs
	cp -R packages/core/minos-microservice-networks/docs/_build/html $(DOCS_TARGET)/core/minos-microservice-networks

	$(MAKE) --directory=packages/core/minos-microservice-saga install docs
	cp -R packages/core/minos-microservice-saga/docs/_build/html $(DOCS_TARGET)/core/minos-microservice-saga

	poetry run $(MAKE) --directory=docs html