Minos Microservice Common
=========================

[![codecov](https://codecov.io/gh/Clariteia/minos_microservice_common/branch/main/graph/badge.svg)](https://codecov.io/gh/Clariteia/minos_microservice_common)

![Tests](https://github.com/Clariteia/minos_microservice_common/actions/workflows/python-tests.yml/badge.svg)

Python Package with common Classes and Utilities used in Minos Microservices

# Run the tests

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

# How to contribute

Minos being an open-source project, we are looking forward to having your contributions. No matter whether it is a pull
request with new features, or the creation of an issue related to a bug you have found.

Please consider these guidelines before you submit any modification.

Create an issue
----------------
1. If you happen to find a bug, please file a new issue filling the 'Bug report' template.
2. Set the appropriate labels, so we can categorise it easily.
3. Wait for any core developer's feedback on it.

Submit a Pull Request
-----------------------
1. Create an issue following the previous steps.
2. Fork the project.
3. Push your changes to a local branch.
4. Run the tests!
5. Submit a pull request from your fork's branch.

# Set up a development environment

Since we use `poetry` as the default package manager, it must be installed. Please refer to
`https://python-poetry.org/docs/#installation`.

Run `pre-commit install`

Make yourself sure you are able to run the tests. Refer to the appropriate section in this guide.

# Credits

This package was created with ![Cookiecutter](https://github.com/audreyr/cookiecutter) and the ![Minos Package](https://github.com/Clariteia/minos-pypackage) project template.

