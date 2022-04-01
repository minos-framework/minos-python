Run the tests
==============

In order to run the tests, please make sure you have the `Docker Engine <https://docs.docker.com/engine/install/>`_
and `Docker Compose <https://docs.docker.com/compose/install/>`_ installed.

Move into tests/ directory

`cd tests/`

Run service dependencies:

`docker-compose up -d`

Install library dependencies:

`make install`

Run tests:

`make test`
