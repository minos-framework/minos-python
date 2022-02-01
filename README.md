<p align="center">
  <a href="http://minos.run" target="_blank"><img src="https://raw.githubusercontent.com/minos-framework/.github/main/images/logo.png" alt="Minos logo"></a>
</p>

# minos-python: The framework which helps you create reactive microservices in Python

[![PyPI Latest Release](https://img.shields.io/pypi/v/minos-microservice-common.svg)](https://pypi.org/project/minos-microservice-common/)
[![GitHub Workflow Status](https://img.shields.io/github/workflow/status/minos-framework/minos-python/pages%20build%20and%20deployment?label=docs)](https://minos-framework.github.io/minos-python)
[![License](https://img.shields.io/github/license/minos-framework/minos-python.svg)](https://github.com/minos-framework/minos-python/blob/main/LICENSE)
[![Coverage](https://codecov.io/github/minos-framework/minos-python/coverage.svg?branch=main)](https://codecov.io/gh/minos-framework/minos-python)
[![Stack Overflow](https://img.shields.io/badge/Stack%20Overflow-Ask%20a%20question-green)](https://stackoverflow.com/questions/tagged/minos)
[![Gitter](https://badges.gitter.im/Join%20Chat.svg)](https://gitter.im/minos-framework/community)
[![Tokei](https://tokei.rs/b1/github/minos-framework/minos-python?category=code)](https://github.com/minos-framework/minos-python)

## Summary

Minos is a framework which helps you create [reactive](https://www.reactivemanifesto.org/) microservices in Python. Internally, it leverages Event Sourcing, CQRS and a message driven architecture to fulfil the commitments of an asynchronous environment.

## Foundational Patterns

The `minos` framework is built strongly following the following set of patterns:

* [Microservice architecture](https://microservices.io/patterns/microservices.html): Architect an application as a collection of loosely coupled services.
* [Decompose by subdomain](https://microservices.io/patterns/decomposition/decompose-by-subdomain.html): Define services corresponding to Domain-Driven Design (DDD) subdomains
* [Self-contained Service](https://microservices.io/patterns/decomposition/self-contained-service.html): Microservices can respond to a synchronous request without waiting for the response from any other service.
* [Database per service](https://microservices.io/patterns/data/database-per-service.html): Keep each microservice's persistent data private to that service and accessible only via its API. A service's transactions only involve its database.
* [Saga](https://microservices.io/patterns/data/saga.html): Transaction that spans multiple services.
* [CQRS](https://microservices.io/patterns/data/cqrs.html): view database, which is a read-only replica that is designed to support queries that retrieves data from microservice. The application keeps the replica up to date by subscribing to Domain events published by the service that own the data.
* [Domain event](https://microservices.io/patterns/data/domain-event.html): A service often needs to publish events when it updates its data. These events might be needed, for example, to update a CQRS view.
* [Event Sourcing](https://microservices.io/patterns/data/event-sourcing.html): Event sourcing persists the state of a business entity such an Order or a Customer as a sequence of state-changing events. Whenever the state of a business entity changes, a new event is appended to the list of events. Since saving an event is a single operation, it is inherently atomic. The application reconstructs an entity's current state by replaying the events.
* [Messaging](https://microservices.io/patterns/communication-style/messaging.html): Services communicating by exchanging messages over messaging channels. (Apache Kafka is used in this case)
* [API gateway](https://microservices.io/patterns/apigateway.html): Single entry point for all clients. The API gateway proxy/route to the appropriate service.
* [Service registry](https://microservices.io/patterns/service-registry.html): Database of services. A service registry might invoke a service instance's health check API to verify that it is able to handle requests
* [Self Registration](https://microservices.io/patterns/self-registration.html): Each service instance register on startup and unregister on stop.
* [Access token](https://microservices.io/patterns/security/access-token.html): The API Gateway authenticates the request and passes an access token (e.g. JSON Web Token) that securely identifies the requestor in each request to the services. A service can include the access token in requests it makes to other services.
* [Health Check API](https://microservices.io/patterns/observability/health-check-api.html): A service has a health check API endpoint (e.g. HTTP `/health`) that returns the health of the service.

## Installation

### Guided installation

The easiest way to use `minos` is with the help of the [`minos-cli`](https://github.com/minos-framework/minos-cli), which provides commands to setup both the project skeleton (configures containerization, databases, brokers, etc.) and the microservice skeleton (the base microservice structure, environment configuration, etc.)

### Manual installation

If you want to directly use `minos` without the command-line utility, the following command will install the needed packages:

```shell
pip install \
  minos-microservice-aggregate \
  minos-microservice-common \
  minos-microservice-cqrs \
  minos-microservice-networks \
  minos-microservice-saga
```

## QuickStart

This section includes a quickstart guide to create your first `minos` microservice, so that anyone can get the gist of the framework.

#### Setting up the environment

The required environment to run this quickstart is the following:

* A `python>=3.9` interpreter with version equal or greater to .
* A `kafka` instance available at `localhost:9092`
* A `postgres` instance available at `localhost:5432` with `foo_db` and `foobar_db` databases accessible with the `minos:min0s` credentials.
* A `socket` available to use at `localhost:4545`.

Note that these parameters can be configured on the `foo.yml` file.

### Configure a Microservice

To keep things simpler, this quickstart will create a microservice assuming all the source code is stored on a single `foo.py` file. In addition to the source file, a `foo.yml` will contain all the configuration stuff.

Create a `foo.yml` file and add the following lines:
<details>
  <summary>Click to show the <code>foo.yml</code></summary>

```yaml
# foo.yml

service:
    name: foo
    aggregate: foo.Foo
    injections:
        lock_pool: minos.common.PostgreSqlLockPool
        postgresql_pool: minos.common.PostgreSqlPool
        broker_publisher: minos.networks.PostgreSqlQueuedKafkaBrokerPublisher
        broker_subscriber_builder: minos.networks.PostgreSqlQueuedKafkaBrokerSubscriberBuilder
        broker_pool: minos.networks.BrokerClientPool
        transaction_repository: minos.aggregate.PostgreSqlTransactionRepository
        event_repository: minos.aggregate.PostgreSqlEventRepository
        snapshot_repository: minos.aggregate.PostgreSqlSnapshotRepository
        saga_manager: minos.saga.SagaManager
    services:
        - minos.networks.BrokerHandlerService
        - minos.networks.RestService
        - minos.networks.PeriodicTaskSchedulerService
middleware:
    - minos.saga.transactional_command
services:
    - minos.aggregate.TransactionService
    - minos.aggregate.SnapshotService
    - minos.saga.SagaService
    - foo.FooCommandService
    - foo.FooQueryService
rest:
    host: 0.0.0.0
    port: 4545
broker:
    host: localhost
    port: 9092
    queue:
        database: foo_db
        user: minos
        password: min0s
        host: localhost
        port: 5432
        records: 1000
        retry: 2
repository:
    database: foo_db
    user: minos
    password: min0s
    host: localhost
    port: 5432
snapshot:
    database: foo_db
    user: minos
    password: min0s
    host: localhost
    port: 5432
saga:
    storage:
        path: "./foo.lmdb"
```

</details>

Create a `foo.py` file and add the following content:

```python
# foo.py

from pathlib import Path
from minos.aggregate import Aggregate
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService


class Foo(Aggregate):
    """Foo Aggregate class."""


class FooCommandService(CommandService):
    """Foo Command Service class."""


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foo.yml")
    launcher.launch()
```

Execute the following command to start the microservice:

```shell
python foo.py
```

### Create an Aggregate

Here is an example of the creation the `Foo` aggregate. In this case, it has two attributes, a `bar` being a `str`, and a `foobar` being an optional reference to the external `FooBar` aggregate, which it is assumed that it has a `something` attribute.

```python
# foo.py

from __future__ import annotations
from typing import Optional
from minos.aggregate import Aggregate, AggregateRef, ModelRef


class Foo(Aggregate):
    """Foo Aggregate class."""

    bar: str
    foobar: Optional[ModelRef[FooBar]]


class FooBar(AggregateRef):
    """FooBar AggregateRef clas."""

    something: str
```
<details>
  <summary>Click to show the full <code>foo.py</code></summary>

```python
# foo.py

from __future__ import annotations

from pathlib import Path
from typing import Optional

from minos.aggregate import Aggregate, AggregateRef, ModelRef
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService


class Foo(Aggregate):
    """Foo Aggregate class."""

    bar: str
    foobar: Optional[ModelRef[FooBar]]


class FooBar(AggregateRef):
    """FooBar AggregateRef clas."""

    something: str


class FooCommandService(CommandService):
    """Foo Command Service class."""


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foo.yml")
    launcher.launch()

```

</details>

### Expose a Command

Here is an example of the definition of a command to create `Foo` instances. To do that, it is necessary to define a `CommandService` that contains the handling function. It will handle both the broker messages sent to the `"CreateFoo"` topic and the rest calls to the `"/foos"` path with the `"POST"` method. In this case, the handling function unpacks the `Request`'s content and then calls the `create` method from the `Aggregate`, which stores the `Foo` instance following an event-driven strategy (it also publishes the `"FooCreated"` event). Finally, a `Response` is returned to be handled by the external caller (another microservice or the API-gateway).

```python
# foo.py

from minos.cqrs import CommandService
from minos.networks import enroute, Request, Response


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.broker.command("CreateFoo")
    @enroute.rest.command("/foos", "POST")
    async def create_foo(self, request: Request) -> Response:
        """Create a new Foo.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: A ``Response`` containing identifier of the already created instance.
        """
        content = await request.content()
        bar = content["bar"]

        foo = await Foo.create(bar)

        return Response({"uuid": foo.uuid})
```

<details>
  <summary>Click to show the full <code>foo.py</code></summary>

```python
# foo.py

from __future__ import annotations

from pathlib import Path
from typing import Optional

from minos.aggregate import Aggregate, AggregateRef, ModelRef
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute


class Foo(Aggregate):
    """Foo Aggregate class."""

    bar: str
    foobar: Optional[ModelRef[FooBar]]


class FooBar(AggregateRef):
    """FooBar AggregateRef clas."""

    something: str


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.broker.command("CreateFoo")
    @enroute.rest.command("/foos", "POST")
    async def create_foo(self, request: Request) -> Response:
        """Create a new Foo.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: A ``Response`` containing identifier of the already created instance.
        """
        content = await request.content()
        bar = content["bar"]

        foo = await Foo.create(bar)

        return Response({"uuid": foo.uuid})


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foo.yml")
    launcher.launch()

```

</details>

Execute the following command to start the microservice:

```shell
python foo.py
```

### Subscribe to an Event and Expose a Query

Here is an example of the event and query handling. In this case, it must be defined on a `QueryService` class. In this case a `"FooCreated"` event is handled (and only a `print` of the content is performed). The event contents typically contains instances of `AggregateDiff` type, which is referred to the difference respect to the previously stored instance. The exposed query is connected to the calls that come from the `"/foos/example"` path and `"GET"` method and a naive string is returned.

*Disclaimer*: A real `QueryService` implementation must populate a query-oriented database based on the events to which is subscribed to, and expose queries performed over that query-oriented database.

```python
# foo.py

from minos.cqrs import QueryService
from minos.networks import enroute, Request, Response


class FooQueryService(QueryService):
    """Foo Query Service class."""

    @enroute.broker.event("FooCreated")
    async def foo_created(self, request: Request) -> None:
        """Handle the "FooCreated" event.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: This method does not return anything.
        """
        diff = await request.content()
        print(f"A Foo was created: {diff}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")
```

<details>
  <summary>Click to show the full <code>foo.py</code></summary>

```python
# foo.py

from __future__ import annotations

from pathlib import Path
from typing import Optional

from minos.aggregate import Aggregate, AggregateRef, ModelRef
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute


class Foo(Aggregate):
    """Foo Aggregate class."""

    bar: str
    foobar: Optional[ModelRef[FooBar]]


class FooBar(AggregateRef):
    """FooBar AggregateRef clas."""

    something: str


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.broker.command("CreateFoo")
    @enroute.rest.command("/foos", "POST")
    async def create_foo(self, request: Request) -> Response:
        """Create a new Foo.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: A ``Response`` containing identifier of the already created instance.
        """
        content = await request.content()
        bar = content["bar"]

        foo = await Foo.create(bar)

        return Response({"uuid": foo.uuid})


class FooQueryService(QueryService):
    """Foo Query Service class."""

    @enroute.broker.event("FooCreated")
    async def foo_created(self, request: Request) -> None:
        """Handle the "FooCreated" event.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: This method does not return anything.
        """
        diff = await request.content()
        print(f"A Foo was created: {diff}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foo.yml")
    launcher.launch()

```

</details>

Execute the following command to start the microservice:

```shell
python foo.py
```

### Interact with another Microservice

Here is an example of the interaction between two microservices through a SAGA pattern. In this case, the interaction starts with a call to the `"/foo/add-foobar"` path and the `"POST"` method, which performs a `SagaManager` run over the `ADD_FOOBAR_SAGA` saga. This saga has two steps, one remote that executes the `"CreateFooBar"` command (possibly defined on the supposed `"foobar"` microservice), and a local step that is executed on this microservice. The `CreateFooBarDTO` defines the structure of the request to be sent when the `"CreateFooBar"` command is executed.

```python
# foo.py

from minos.common import ModelType
from minos.cqrs import CommandService
from minos.networks import enroute, Request
from minos.saga import Saga, SagaContext, SagaRequest, SagaResponse


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.rest.command("/foo/add-foobar", "POST")
    async def update_foo(self, request: Request) -> None:
        """Run a saga example.

        :param request: The ``Request`` that contains the initial saga's context.
        :return: This method does not return anything.
        """
        content = await request.content()

        await self.saga_manager.run(
            ADD_FOOBAR_SAGA, SagaContext(uuid=content["uuid"], something=content["something"])
        )


CreateFooBarDTO = ModelType.build("AnotherDTO", {"number": int, "text": str})


def _create_foobar(context: SagaContext) -> SagaRequest:
    something = context["something"]
    content = CreateFooBarDTO(56, something)
    return SagaRequest("CreateFooBar", content)


async def _success_foobar(context: SagaContext, response: SagaResponse) -> SagaContext:
    context["foobar_uuid"] = await response.content()
    return context


async def _error_foobar(context: SagaContext, response: SagaResponse) -> SagaContext:
    raise ValueError("The foobar could not be created!")


async def _update_foo(context: SagaContext) -> None:
    foo = await Foo.get(context["uuid"])
    foo.foobar = context["foobar_uuid"]
    await foo.save()


ADD_FOOBAR_SAGA = (
    Saga()
        .remote_step().on_execute(_create_foobar).on_success(_success_foobar).on_error(_error_foobar)
        .local_step().on_execute(_update_foo)
        .commit()
)
```
<details>
  <summary>Click to show the full <code>foo.py</code></summary>

```python
# foo.py

from __future__ import annotations

from pathlib import Path
from typing import Optional

from minos.aggregate import Aggregate, AggregateRef, ModelRef
from minos.common import ModelType, EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute
from minos.saga import Saga, SagaContext, SagaRequest, SagaResponse


class Foo(Aggregate):
    """Foo Aggregate class."""

    bar: str
    foobar: Optional[ModelRef[FooBar]]


class FooBar(AggregateRef):
    """FooBar AggregateRef clas."""

    something: str


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.broker.command("CreateFoo")
    @enroute.rest.command("/foos", "POST")
    async def create_foo(self, request: Request) -> Response:
        """Create a new Foo.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: A ``Response`` containing identifier of the already created instance.
        """
        content = await request.content()
        bar = content["bar"]

        foo = await Foo.create(bar)

        return Response({"uuid": foo.uuid})

    @enroute.rest.command("/foo/add-foobar", "POST")
    async def update_foo(self, request: Request) -> None:
        """Run a saga example.

        :param request: The ``Request`` that contains the initial saga's context.
        :return: This method does not return anything.
        """
        content = await request.content()

        await self.saga_manager.run(
            ADD_FOOBAR_SAGA, SagaContext(uuid=content["uuid"], something=content["something"])
        )


CreateFooBarDTO = ModelType.build("AnotherDTO", {"number": int, "text": str})


def _create_foobar(context: SagaContext) -> SagaRequest:
    something = context["something"]
    content = CreateFooBarDTO(56, something)
    return SagaRequest("CreateFooBar", content)


async def _success_foobar(context: SagaContext, response: SagaResponse) -> SagaContext:
    context["foobar_uuid"] = await response.content()
    return context


async def _error_foobar(context: SagaContext, response: SagaResponse) -> SagaContext:
    raise ValueError("The foobar could not be created!")


async def _update_foo(context: SagaContext) -> None:
    foo = await Foo.get(context["uuid"])
    foo.foobar = context["foobar_uuid"]
    await foo.save()


ADD_FOOBAR_SAGA = (
    Saga()
        .remote_step()
        .on_execute(_create_foobar)
        .on_success(_success_foobar)
        .on_error(_error_foobar)
        .local_step()
        .on_execute(_update_foo)
        .commit()
)


class FooQueryService(QueryService):
    """Foo Query Service class."""

    @enroute.broker.event("FooCreated")
    async def foo_created(self, request: Request) -> None:
        """Handle the "FooCreated" event.

        :param request: The ``Request`` that contains the ``bar`` attribute.
        :return: This method does not return anything.
        """
        diff = await request.content()
        print(f"A Foo was created: {diff}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foo.yml")
    launcher.launch()

```

</details>

Execute the following command to start the `foo` microservice:

```shell
python foo.py
```

**Disclaimer**: Note that in this case another microservice is needed to complete the saga.

#### The `foobar` Microservice

Here is the `foobar.yml` config file:
<details>
  <summary>Click to show the <code>foobar.yml</code></summary>

```yaml
# foobar.yml

service:
    name: foobar
    aggregate: foobar.FooBar
    injections:
        lock_pool: minos.common.PostgreSqlLockPool
        postgresql_pool: minos.common.PostgreSqlPool
        broker_publisher: minos.networks.PostgreSqlQueuedKafkaBrokerPublisher
        broker_subscriber_builder: minos.networks.PostgreSqlQueuedKafkaBrokerSubscriberBuilder
        broker_pool: minos.networks.BrokerClientPool
        transaction_repository: minos.aggregate.PostgreSqlTransactionRepository
        event_repository: minos.aggregate.PostgreSqlEventRepository
        snapshot_repository: minos.aggregate.PostgreSqlSnapshotRepository
        saga_manager: minos.saga.SagaManager
    services:
        - minos.networks.BrokerHandlerService
        - minos.networks.RestService
        - minos.networks.PeriodicTaskSchedulerService
middleware:
    - minos.saga.transactional_command
services:
    - minos.aggregate.TransactionService
    - minos.aggregate.SnapshotService
    - minos.saga.SagaService
    - foobar.FooBarCommandService
rest:
    host: 0.0.0.0
    port: 4546
broker:
    host: localhost
    port: 9092
    queue:
        database: foobar_db
        user: minos
        password: min0s
        host: localhost
        port: 5432
        records: 1000
        retry: 2
repository:
    database: foobar_db
    user: minos
    password: min0s
    host: localhost
    port: 5432
snapshot:
    database: foobar_db
    user: minos
    password: min0s
    host: localhost
    port: 5432
saga:
    storage:
        path: "./foobar.lmdb"
```

</details>

Here is the `foobar.py` config file:
<details>
  <summary>Click to show the <code>foobar.py</code></summary>

```python
from __future__ import annotations

from pathlib import Path

from minos.aggregate import Aggregate
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService
from minos.networks import Request, Response, enroute


class FooBar(Aggregate):
    """FooBar AggregateRef clas."""

    something: str


class FooBarCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.broker.command("CreateFooBar")
    async def create_foobar(self, request: Request) -> Response:
        """Create a new FooBar.

        :param request: The ``Request`` that contains the ``something`` attribute.
        :return: A ``Response`` containing identifier of the already created instance.
        """
        content = await request.content()
        something = content["text"]

        foobar = await FooBar.create(something)

        return Response(foobar.uuid)


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "foobar.yml")
    launcher.launch()

```

</details>

Execute the following command to start the `foobar` microservice:

```shell
python foobar.py
```

## Packages

This project follows a modular structure based on python packages.

### Core

The core packages provide the base implementation of the framework.

* [minos-microservice-aggregate](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-aggregate): The Aggregate pattern implementation.
* [minos-microservice-common](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-common): The common core.
* [minos-microservice-cqrs](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-cqrs): The CQRS pattern implementation.
* [minos-microservice-networks](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-networks): The networks core.
* [minos-microservice-saga](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-saga): The SAGA pattern implementation.

### Plugins

The plugin packages provide connectors to external technologies like brokers, discovery services, databases, serializers and so on.

* Coming soon...

## Documentation

The official API Reference is publicly available at the [GitHub Pages](https://minos-framework.github.io/minos-python).

## Source Code

The source code of this project is hosted at the [GitHub Repository](https://github.com/minos-framework/minos-python).

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
