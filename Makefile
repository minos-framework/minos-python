.PHONY: install docs

DOCS_TARGET := ./docs/api-reference/packages

install:
	poetry install

docs:
	mkdir -p $(DOCS_TARGET)

	$(MAKE) --directory=packages/minos-microservice-aggregate install docs
	cp -R packages/minos-microservice-aggregate/docs/_build/html $(DOCS_TARGET)/minos-microservice-aggregate

	$(MAKE) --directory=packages/minos-microservice-common install docs
	cp -R packages/minos-microservice-common/docs/_build/html $(DOCS_TARGET)/minos-microservice-common

	$(MAKE) --directory=packages/minos-microservice-cqrs install docs
	cp -R packages/minos-microservice-cqrs/docs/_build/html $(DOCS_TARGET)/minos-microservice-cqrs

	$(MAKE) --directory=packages/minos-microservice-networks install docs
	cp -R packages/minos-microservice-networks/docs/_build/html $(DOCS_TARGET)/minos-microservice-networks

	$(MAKE) --directory=packages/minos-microservice-saga install docs
	cp -R packages/minos-microservice-saga/docs/_build/html $(DOCS_TARGET)/minos-microservice-saga

	poetry run $(MAKE) --directory=docs html