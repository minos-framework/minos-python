# Minos Microservice Saga

[![codecov](https://codecov.io/gh/Clariteia/minos_microservice_saga/branch/main/graph/badge.svg)](https://codecov.io/gh/Clariteia/minos_microservice_saga)
![Tests](https://github.com/Clariteia/minos_microservice_saga/actions/workflows/python-tests.yml/badge.svg)

Minos is a framework which helps you create [reactive](https://www.reactivemanifesto.org/) microservices in Python.
Internally, it leverages Event Sourcing, CQRS and a message driven architecture to fulfil the commitments of an
asynchronous environment.

## Documentation

The official documentation as well as the API you can find it under https://clariteia.github.io/minos_microservice_saga/. 
Please, submit any issue regarding documentation as well!

## Set up a development environment

Minos uses `poetry` as its default package manager. Please refer to the
[Poetry installation guide](https://python-poetry.org/docs/#installation) for instructions on how to install it.

Now you con install all the dependencies by running
```bash
make install
```

In order to make the pre-commits checks available to git, run
```bash
pre-commit install
```

Make yourself sure you are able to run the tests. Refer to the appropriate section in this guide.

## Run the tests

In order to run the tests, please make sure you have the [Docker Engine](https://docs.docker.com/engine/install/)
and [Docker Compose](https://docs.docker.com/compose/install/) installed.

Move into `tests/` directory

```bash
cd tests/
```
Run service dependencies:

```bash
docker-compose up -d
```

Install library dependencies:

```bash
make install
```

Run tests:

```bash
make test
```

## How to contribute

Minos being an open-source project, we are looking forward to having your contributions. No matter whether it is a pull
request with new features, or the creation of an issue related to a bug you have found.

Please consider these guidelines before you submit any modification.

### Create an issue

1. If you happen to find a bug, please file a new issue filling the 'Bug report' template.
2. Set the appropriate labels, so we can categorise it easily.
3. Wait for any core developer's feedback on it.

### Submit a Pull Request

1. Create an issue following the previous steps.
2. Fork the project.
3. Push your changes to a local branch.
4. Run the tests!
5. Submit a pull request from your fork's branch.

## Credits

This package was created with ![Cookiecutter](https://github.com/audreyr/cookiecutter) and the ![Minos Package](https://github.com/Clariteia/minos-pypackage) project template.
