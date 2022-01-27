
<p align="center">
  <a href="http://minos.run" target="_blank"><img src="https://raw.githubusercontent.com/minos-framework/.github/main/images/logo.png" alt="Minos logo"></a>
</p>

# minos-python: The framework which helps you create reactive microservices in Python
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-aggregate.svg?label=minos-microservice-aggregate)](https://pypi.org/project/minos-microservice-aggregate/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-common.svg?label=minos-microservice-common)](https://pypi.org/project/minos-microservice-common/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-cqrs.svg?label=minos-microservice-cqrs)](https://pypi.org/project/minos-microservice-cqrs/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-networks.svg?label=minos-microservice-networks)](https://pypi.org/project/minos-microservice-networks/)
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-saga.svg?label=minos-microservice-saga)](https://pypi.org/project/minos-microservice-saga/) 
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/minos-framework/minos-python/pages%20build%20and%20deployment?label=docs)](https://minos-framework.github.io/minos-python)
[![License](https://img.shields.io/github/license/minos-framework/minos-python.svg)](https://github.com/minos-framework/minos-python/blob/main/LICENSE)
[![Coverage](https://codecov.io/github/minos-framework/minos-python/coverage.svg?branch=main)](https://codecov.io/gh/minos-framework/minos-python)
[![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-Ask%20a%20question-green)](https://stackoverflow.com/questions/tagged/minos)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minos-framework/community)

## Summary

Minos is a framework which helps you create [reactive](https://www.reactivemanifesto.org/) microservices in Python.
Internally, it leverages Event Sourcing, CQRS and a message driven architecture to fulfil the commitments of an
asynchronous environment.

## Documentation

The official API Reference is publicly available at [GiHub Pages](https://minos-framework.github.io/minos-python).

## Packages

This project follows a modular structure based on python packages.

### Core

The core packages provide the base implementation of the framework.

* [minos-microservice-aggregate](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-aggregate): The implementation of the Aggregate pattern.
* [minos-microservice-common](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-common): The implementation of common classes and utilities to be used by another packages.
* [minos-microservice-cqrs](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-cqrs): The implementation of the CQRS pattern.
* [minos-microservice-networks](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-networks): The implementation of the rest server, the broker client, the discovery connector, etc.
* [minos-microservice-saga](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-saga): The implementation of the SAGA pattern.

### Plugins

The plugin packages provide connectors to external technologies like brokers, discovery services, databases, serializers and so on. 

## Source Code

The source code of this project is hosted at [GitHub](https://github.com/minos-framework/minos-python). 

## Getting Help

For usage questions, the best place to go to is [StackOverflow](https://stackoverflow.com/questions/tagged/minos).

## Discussion and Development
Most development discussions take place over the [GitHub Issues](https://github.com/minos-framework/minos-python/issues). In addition, a [Gitter channel](https://gitter.im/minos-framework/community) is available for development-related questions.

## How to contribute

We are looking forward to having your contributions. No matter whether it is a pull request with new features, or the creation of an issue related to a bug you have found.

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

## License

This project is distributed under the [MIT](https://raw.githubusercontent.com/minos-framework/minos-python/main/LICENSE) license.
