.PHONY: docs

docs:
	$(MAKE) --directory=packages/minos-microservice-aggregate docs
	$(MAKE) --directory=packages/minos-microservice-common docs
	$(MAKE) --directory=packages/minos-microservice-cqrs docs
	$(MAKE) --directory=packages/minos-microservice-networks docs
	$(MAKE) --directory=packages/minos-microservice-saga docs