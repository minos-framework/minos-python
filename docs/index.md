<p align="center">
  <a href="https://minos.run" target="_blank"><img src="https://raw.githubusercontent.com/minos-framework/.github/main/images/logo.png" alt="Minos logo"></a>
</p>

# minos-python: The framework which helps you create reactive microservices in Python

[![GitHub Repo stars](https://img.shields.io/github/stars/minos-framework/minos-python?label=GitHub)](https://github.com/minos-framework/minos-python/stargazers)
[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-common.svg)](https://pypi.org/project/minos-microservice-common/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/minos-framework/minos-python/pages%20build%20and%20deployment?label=docs)](https://minos-framework.github.io/minos-python)
[![License](https://img.shields.io/github/license/minos-framework/minos-python.svg)](https://github.com/minos-framework/minos-python/blob/main/LICENSE)
[![Coverage](https://codecov.io/github/minos-framework/minos-python/coverage.svg?branch=main)](https://codecov.io/gh/minos-framework/minos-python)
[![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-Ask%20a%20question-green)](https://stackoverflow.com/questions/tagged/minos)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minos-framework/community)
[![Tokei](https://tokei.rs/b1/github/minos-framework/minos-python?category=code)](https://github.com/minos-framework/minos-python)

## Summary

Minos is a framework which helps you create [reactive](https://www.reactivemanifesto.org/) microservices in Python. Internally, it leverages Event Sourcing, CQRS and a message driven architecture to fulfil the commitments of an asynchronous environment.


## API Reference

.. autosummary::
   :toctree: api-reference
   :recursive:

   minos.aggregate
   minos.common
   minos.cqrs
   minos.networks
   minos.saga
   minos.plugins.kafka
   minos.plugins.rabbitmq
   minos.plugins.minos_discovery
   minos.plugins.aiopg
   minos.plugins.lmdb
   minos.plugins.kong
   minos.plugins.aiohttp
   minos.plugins.graphql

## Indices and tables
* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
