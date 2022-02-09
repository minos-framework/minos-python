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

### Roadmap

#### 0.6.x

* [#78](https://github.com/minos-framework/minos-python/issues/78) Implement a circuit breaker for `minos-broker-kafka`.
* [#87](https://github.com/minos-framework/minos-python/issues/87) Implement idempotency for `BrokerSubscriber` message processing.
* [#100](https://github.com/minos-framework/minos-python/issues/100) Create the `minos-serializers-avro` plugin.
* [#148](https://github.com/minos-framework/minos-python/issues/148) Create the `minos-rest-aiohttp`.
* [#149](https://github.com/minos-framework/minos-python/issues/149) Add `minos-graphql-aiohttp` plugin.
* [#150](https://github.com/minos-framework/minos-python/issues/150) Refactor configuration file and `MinosConfig` accessor.
* [#151](https://github.com/minos-framework/minos-python/issues/151) Expose `OpenAPI` and `AsyncAPI` specifications.
* [#152](https://github.com/minos-framework/minos-python/issues/152) Provide a testing suite to test the microservice.

## Foundational Patterns

The `minos` framework is built strongly inspired by the following set of patterns:

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

The easiest way to manage a project is with the `minos` command-line interface, which provides commands to setup both the project skeleton (configures containerization, databases, brokers, etc.) and the microservice skeleton (the base microservice structure, environment configuration, etc.).

You can install it with:

```shell
pip install minos-cli
```

Here is a summary containing the most useful commands:

* `minos new project $NAME`: Create a new Project
* `minos set $RESOURCE $BACKEND`: Configure an environment resource (broker, database, etc.).
* `minos deploy project`: Deploy a project.
* `minos new microservice $NAME`: Create a new microservice.
* `minos deploy microservice` deploy a microservice.

For more information, visit the [`minos-cli`](https://github.com/minos-framework/minos-cli) repository.

## Documentation

The best place to start learning how to use the Minos Framework is at [Minos Learn](http://www.minos.run/learn/). The official API Reference is publicly available at the [GitHub Pages](https://minos-framework.github.io/minos-python).

## QuickStart

This section includes a quickstart guide to create your first `minos` microservice, so that anyone can get the gist of the framework.

### Set up the environment

The required environment to run this quickstart is the following:

* A `python>=3.9` interpreter with version equal or greater to .
* A `kafka` instance available at `localhost:9092`
* A `postgres` instance available at `localhost:5432` with the `foo_db` and `foobar_db` databases accessible with the `user:pass` credentials.
* Two TCP sockets available to use at `localhost:4545` and `localhost:4546`.


<details>
  <summary>Click to show a <code>docker-compose.yml</code> that provides the <code>kafka</code> and <code>postgres</code> instances ready to be used!</summary>

```yaml
# docker-compose.yml
version: "3.9"
services:
  zookeeper:
    restart: always
    image: wurstmeister/zookeeper:latest
  kafka:
    restart: always
    image: wurstmeister/kafka:latest
    ports:
      - "9092:9092"
    depends_on:
      - zookeeper
    environment:
      KAFKA_ADVERTISED_HOST_NAME: localhost
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
  postgres:
    restart: always
    image: postgres:latest
    ports:
      - "5432:5432"
    environment:
      - POSTGRES_USER=user
      - POSTGRES_PASSWORD=pass
```

Then, start the environment:

```shell
docker-compose up
```

To create the databases, just run the following command:

```shell
docker-compose exec postgres psql -U user -tc 'CREATE database foo_db'
docker-compose exec postgres psql -U user -tc 'CREATE database foobar_db'
```
</details>

Note that these parameters can be customized on the configuration files.

### Install the dependencies

If you want to directly use `minos` without the command-line utility, the following command will install the needed packages:

```shell
pip install \
  minos-microservice-aggregate \
  minos-microservice-common \
  minos-microservice-cqrs \
  minos-microservice-networks \
  minos-microservice-saga \ 
  minos-broker-kafka
```

### Configure a Microservice

To keep things simpler, this quickstart will create a microservice assuming all the source code is stored on a single `foo/main.py` file. In addition to the source file, a `foo/config.yml` will contain all the configuration stuff.

The directory structure will become:

```shell
.
└── foo
    ├── config.yml
    └── main.py
```

Create a `foo/config.yml` file and add the following lines:
<details>
  <summary>Click to show the full file</summary>

```yaml
# foo/config.yml

service:
  name: foo
  aggregate: main.Foo
  injections:
    lock_pool: minos.common.PostgreSqlLockPool
    postgresql_pool: minos.common.PostgreSqlPool
    broker_publisher: minos.plugins.kafka.PostgreSqlQueuedKafkaBrokerPublisher
    broker_subscriber_builder: minos.plugins.kafka.PostgreSqlQueuedKafkaBrokerSubscriberBuilder
    broker_pool: minos.networks.BrokerClientPool
    transaction_repository: minos.aggregate.PostgreSqlTransactionRepository
    event_repository: minos.aggregate.PostgreSqlEventRepository
    snapshot_repository: minos.aggregate.PostgreSqlSnapshotRepository
    saga_manager: minos.saga.SagaManager
    discovery: minos.networks.DiscoveryConnector
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
  - main.FooCommandService
  - main.FooQueryService
rest:
  host: 0.0.0.0
  port: 4545
broker:
  host: localhost
  port: 9092
  queue:
    database: foo_db
    user: user
    password: pass
    host: localhost
    port: 5432
    records: 1000
    retry: 2
repository:
  database: foo_db
  user: user
  password: pass
  host: localhost
  port: 5432
snapshot:
  database: foo_db
  user: user
  password: pass
  host: localhost
  port: 5432
saga:
  storage:
    path: "./foo.lmdb"
discovery:
  client: minos.networks.InMemoryDiscoveryClient
  host: localhost
  port: 5567
```

</details>

Create a `foo/main.py` file and add the following content:

```python
# foo/main.py

from pathlib import Path
from minos.aggregate import Aggregate, RootEntity
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService


class Foo(RootEntity):
    """Foo RootEntity class."""


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""


class FooCommandService(CommandService):
    """Foo Command Service class."""


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()
```

Execute the following command to start the microservice:

```shell
python foo/main.py
```

### Create an Aggregate

The way to model data in `minos` is highly inspired by the  [Event Sourcing](https://microservices.io/patterns/data/event-sourcing.html) ideas. For this reason, the classes to be used to model data are:

* `minos.aggregate.Entity`: A model that has an identifier that gives it a unique identity, in the sense that some values from which it is composed could change, but its identity will continue being the same.
* `minos.aggregate.ExternalEntity`: A model that belongs to another microservice (or aggregate boundary) but needs to be used for some reason inside this microservice (or aggregate boundary).
* `minos.aggregate.RootEntity`: Is an `Entity` superset that provides global identity across the project compared to standard `Entity` models, that has only local identity (the `RootEntity` can be accessed from another microservices as `ExternalEntity` models, but standard `Entity` models can only be accessed within the microservice that define them). The `RootEntity` is also the one that interacts with the persistence layer (the `EventRepository` and `SnapshotRepository` instances).
* `minos.aggregate.Ref`: A wrapper class that provides the functionality to store a reference of other `RootEntity` or `ExternalEntity` instances.
* `minos.aggregate.EntitySet`: A container of `Entity` instances that takes advantage of the incremental behaviour of the `EventRepository`.
* `minos.aggregate.ValueObject`: A model that is only identified by the values that compose it, so that if some of them changes, then the model becomes completely different (for that reason, these models are immutable).
* `minos.aggregate.ValueObjectSet`: A container of `ValueObject` instances that takes advantage of the incremental behaviour of the `EventRepository.
* `minos.aggregate.Aggregate`: A collection of `Entity` and/or `ValueObject` models that are related to each other through a `RootEntity`.
* `minos.aggregate.Event`: A model that contains the difference between the a `RootEntity` instance and its previous version (if any).

Here is an example of the creation the `Foo` aggregate. In this case, it has two attributes, a `bar` being a `str`, and a `foobar` being an optional reference to the external `FooBar` aggregate, which it is assumed that it has a `something` attribute.

```python
# foo/main.py

from __future__ import annotations
from typing import Optional
from uuid import UUID
from minos.aggregate import Aggregate, RootEntity, ExternalEntity, Ref


class Foo(RootEntity):
    """Foo RootEntity class."""

    bar: str
    foobar: Optional[Ref[FooBar]]


class FooBar(ExternalEntity):
    """FooBar ExternalEntity clas."""

    something: str


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""

    @staticmethod
    async def create_foo(bar: str) -> UUID:
        """Create a new Foo instance
        
        :param bar: The bar of the new instance.
        :return: The identifier of the new instance.
        """
        foo = await Foo.create(bar)

        return foo.uuid

    @staticmethod
    async def update_foobar(uuid: UUID, foobar: Optional[Ref[FooBar]]) -> None:
        """Update the foobar attribute of the ``Foo`` instance.
        
        :param uuid: The identifier of the ``Foo`` instance.
        :param foobar: The foobar value to be set.
        :return: This method does not return anything.
        """
        foo = await Foo.get(uuid)
        foo.foobar = foobar
        await foo.save()
```

<details>
  <summary>Click to show the full file</summary>

```python
# foo/main.py

from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import UUID

from minos.aggregate import Aggregate, RootEntity, ExternalEntity, Ref
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService


class Foo(RootEntity):
    """Foo RootEntity class."""

    bar: str
    foobar: Optional[Ref[FooBar]]


class FooBar(ExternalEntity):
    """FooBar ExternalEntity clas."""

    something: str


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""

    @staticmethod
    async def create_foo(bar: str) -> UUID:
        """Create a new Foo instance
        
        :param bar: The bar of the new instance.
        :return: The identifier of the new instance.
        """
        foo = await Foo.create(bar)

        return foo.uuid

    @staticmethod
    async def update_foobar(uuid: UUID, foobar: Optional[Ref[FooBar]]) -> None:
        """Update the foobar attribute of the ``Foo`` instance.
        
        :param uuid: The identifier of the ``Foo`` instance.
        :param foobar: The foobar value to be set.
        :return: This method does not return anything.
        """
        foo = await Foo.get(uuid)
        foo.foobar = foobar
        await foo.save()


class FooCommandService(CommandService):
    """Foo Command Service class."""


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()
```

</details>

### Expose a Command

Here is an example of the definition of a command to create `Foo` instances. To do that, it is necessary to define a `CommandService` that contains the handling function. It will handle both the broker messages sent to the `"CreateFoo"` topic and the rest calls to the `"/foos"` path with the `"POST"` method. In this case, the handling function unpacks the `Request`'s content and then calls the `create` method from the `Aggregate`, which stores the `Foo` instance following an event-driven strategy (it also publishes the `"FooCreated"` event). Finally, a `Response` is returned to be handled by the external caller (another microservice or the API-gateway).

```python
# foo/main.py

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

        uuid = await FooAggregate.create_foo(bar)

        return Response({"uuid": uuid})
```

<details>
  <summary>Click to show the full file</summary>

```python
# foo/main.py

from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import UUID

from minos.aggregate import Aggregate, RootEntity, ExternalEntity, Ref
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute


class Foo(RootEntity):
    """Foo RootEntity class."""

    bar: str
    foobar: Optional[Ref[FooBar]]


class FooBar(ExternalEntity):
    """FooBar ExternalEntity clas."""

    something: str


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""

    @staticmethod
    async def create_foo(bar: str) -> UUID:
        """Create a new Foo instance
        
        :param bar: The bar of the new instance.
        :return: The identifier of the new instance.
        """
        foo = await Foo.create(bar)

        return foo.uuid

    @staticmethod
    async def update_foobar(uuid: UUID, foobar: Optional[Ref[FooBar]]) -> None:
        """Update the foobar attribute of the ``Foo`` instance.
        
        :param uuid: The identifier of the ``Foo`` instance.
        :param foobar: The foobar value to be set.
        :return: This method does not return anything.
        """
        foo = await Foo.get(uuid)
        foo.foobar = foobar
        await foo.save()


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

        uuid = await FooAggregate.create_foo(bar)

        return Response({"uuid": uuid})


class FooQueryService(QueryService):
    """Foo Query Service class."""


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()
```

</details>

Execute the following command to start the microservice:

```shell
python foo/main.py
```

To check that everything works fine, execute the following command:

```shell
curl --location --request POST 'http://localhost:4545/foos' \
--header 'Content-Type: application/json' \
--data-raw '{
    "bar": "test"
}'
```

And the expected response will be similar to:

```json
{
  "uuid": "YOUR_UUID"
}
```

### Subscribe to an Event and Expose a Query

Here is an example of the event and query handling. In this case, it must be defined on a `QueryService` class. In this case a `"FooCreated"` and `"FooUpdated.foobar"` events are handled (they will print the content on the microservice's logs). The event contents typically contains instances of `AggregateDiff` type, which is referred to the difference respect to the previously stored instance. The exposed query is connected to the calls that come from the `"/foos/example"` path and `"GET"` method and a naive string is returned.

*Disclaimer*: A real `QueryService` implementation must populate a query-oriented database based on the events to which is subscribed to, and expose queries performed over that query-oriented database.

```python
# foo/main.py

from minos.cqrs import QueryService
from minos.networks import enroute, Request, Response


class FooQueryService(QueryService):
    """Foo Query Service class."""

    @enroute.broker.event("FooCreated")
    async def foo_created(self, request: Request) -> None:
        """Handle the "FooCreated" event.

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"A Foo was created: {event}")

    @enroute.broker.event("FooUpdated.foobar")
    async def foo_foobar_updated(self, request: Request) -> None:
        """Handle the "FooUpdated.foobar" event.

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"The 'foobar' field of a Foo was updated: {event}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")
```

<details>
  <summary>Click to show the full file</summary>

```python
# foo/main.py

from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import UUID

from minos.aggregate import Aggregate, RootEntity, ExternalEntity, Ref
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute


class Foo(RootEntity):
    """Foo RootEntity class."""

    bar: str
    foobar: Optional[Ref[FooBar]]


class FooBar(ExternalEntity):
    """FooBar ExternalEntity clas."""

    something: str


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""

    @staticmethod
    async def create_foo(bar: str) -> UUID:
        """Create a new Foo instance
        
        :param bar: The bar of the new instance.
        :return: The identifier of the new instance.
        """
        foo = await Foo.create(bar)

        return foo.uuid

    @staticmethod
    async def update_foobar(uuid: UUID, foobar: Optional[Ref[FooBar]]) -> None:
        """Update the foobar attribute of the ``Foo`` instance.
        
        :param uuid: The identifier of the ``Foo`` instance.
        :param foobar: The foobar value to be set.
        :return: This method does not return anything.
        """
        foo = await Foo.get(uuid)
        foo.foobar = foobar
        await foo.save()


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

        uuid = await FooAggregate.create_foo(bar)

        return Response({"uuid": uuid})


class FooQueryService(QueryService):
    """Foo Query Service class."""

    @enroute.broker.event("FooCreated")
    async def foo_created(self, request: Request) -> None:
        """Handle the "FooCreated" event.

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"A Foo was created: {event}")

    @enroute.broker.event("FooUpdated.foobar")
    async def foo_foobar_updated(self, request: Request) -> None:
        """Handle the "FooUpdated.foobar" event.

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"The 'foobar' field of a Foo was updated: {event}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()
```

</details>

Execute the following command to start the microservice:

```shell
python foo/main.py
```

Now, if a new instance is created (with a rest call, like in the [previous section](#expose-a-command)), the `FooCreated` event will be handled and the microservice's console will print something like:

```
A Foo was created: Event(...)
```

Also, to check that everything is fine the example query can be executed with:

```shell
curl --location --request GET 'http://localhost:4545/foos/example'
```

And the expected result should be something like:

```
"This is an example response!"
```

### Interact with another Microservice

Here is an example of the interaction between two microservices through a SAGA pattern. In this case, the interaction starts with a call to the `"/foos/add-foobar"` path and the `"POST"` method, which performs a `SagaManager` run over the `ADD_FOOBAR_SAGA` saga. This saga has two steps, one remote that executes the `"CreateFooBar"` command (possibly defined on the supposed `"foobar"` microservice), and a local step that is executed on this microservice. The `CreateFooBarDTO` defines the structure of the request to be sent when the `"CreateFooBar"` command is executed.

```python
# foo/main.py

from minos.common import ModelType
from minos.cqrs import CommandService
from minos.networks import enroute, Request
from minos.saga import Saga, SagaContext, SagaRequest, SagaResponse


class FooCommandService(CommandService):
    """Foo Command Service class."""

    @enroute.rest.command("/foos/add-foobar", "POST")
    async def update_foo(self, request: Request) -> None:
        """Run a saga example.

        :param request: The ``Request`` that contains the initial saga's context.
        :return: This method does not return anything.
        """
        content = await request.content()

        context = SagaContext(uuid=content["uuid"], something=content["something"])
        await self.saga_manager.run(ADD_FOOBAR_SAGA, context)


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
    await FooAggregate.update_foobar(context["uuid"], context["foobar_uuid"])


CreateFooBarDTO = ModelType.build("AnotherDTO", {"number": int, "text": str})

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
```

<details>
  <summary>Click to show the full file</summary>

```python
# foo/main.py

from __future__ import annotations

from pathlib import Path
from typing import Optional
from uuid import UUID

from minos.aggregate import Aggregate, RootEntity, ExternalEntity, Ref
from minos.common import ModelType, EntrypointLauncher
from minos.cqrs import CommandService, QueryService
from minos.networks import Request, Response, enroute
from minos.saga import Saga, SagaContext, SagaRequest, SagaResponse


class Foo(RootEntity):
    """Foo RootEntity class."""

    bar: str
    foobar: Optional[Ref[FooBar]]


class FooBar(ExternalEntity):
    """FooBar ExternalEntity clas."""

    something: str


class FooAggregate(Aggregate[Foo]):
    """Foo Aggregate class."""

    @staticmethod
    async def create_foo(bar: str) -> UUID:
        """Create a new Foo instance
        
        :param bar: The bar of the new instance.
        :return: The identifier of the new instance.
        """
        foo = await Foo.create(bar)

        return foo.uuid

    @staticmethod
    async def update_foobar(uuid: UUID, foobar: Optional[Ref[FooBar]]) -> None:
        """Update the foobar attribute of the ``Foo`` instance.
        
        :param uuid: The identifier of the ``Foo`` instance.
        :param foobar: The foobar value to be set.
        :return: This method does not return anything.
        """
        foo = await Foo.get(uuid)
        foo.foobar = foobar
        await foo.save()


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

        uuid = await FooAggregate.create_foo(bar)

        return Response({"uuid": uuid})

    @enroute.rest.command("/foos/add-foobar", "POST")
    async def update_foo(self, request: Request) -> None:
        """Run a saga example.

        :param request: The ``Request`` that contains the initial saga's context.
        :return: This method does not return anything.
        """
        content = await request.content()

        context = SagaContext(uuid=content["uuid"], something=content["something"])
        await self.saga_manager.run(ADD_FOOBAR_SAGA, context)


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
    await FooAggregate.update_foobar(context["uuid"], context["foobar_uuid"])


CreateFooBarDTO = ModelType.build("AnotherDTO", {"number": int, "text": str})

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

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"A Foo was created: {event}")

    @enroute.broker.event("FooUpdated.foobar")
    async def foo_foobar_updated(self, request: Request) -> None:
        """Handle the "FooUpdated.foobar" event.

        :param request: The ``Request`` that contains a ``Event``.
        :return: This method does not return anything.
        """
        event = await request.content()
        print(f"The 'foobar' field of a Foo was updated: {event}")

    @enroute.rest.query("/foos/example", "GET")
    async def example(self, request: Request) -> Response:
        """Handle the example query.

        :param request: The ``Request`` that contains the necessary information.
        :return: A ``Response`` instance.
        """
        return Response("This is an example response!")


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()
```

</details>

Execute the following command to start the `foo` microservice:

```shell
python foo/main.py
```

**Disclaimer**: Note that in this case another microservice is needed to complete the saga.

#### The `foobar` Microservice

The `foobar` microservice will simply have a `CreateFooBar` command to create new instances of its `FooBar` root entity.

The directory structure will become:

```shell
.
├── foo
│   ├── config.yml
│   └── main.py
└── foobar
    ├── config.yml
    └── main.py
```

Here is the `foobar/config.yml` config file:
<details>
  <summary>Click to show the full file</summary>

```yaml
# foobar/config.yml

service:
  name: foobar
  aggregate: main.FooBar
  injections:
    lock_pool: minos.common.PostgreSqlLockPool
    postgresql_pool: minos.common.PostgreSqlPool
    broker_publisher: minos.plugins.kafka.PostgreSqlQueuedKafkaBrokerPublisher
    broker_subscriber_builder: minos.plugins.kafka.PostgreSqlQueuedKafkaBrokerSubscriberBuilder
    broker_pool: minos.networks.BrokerClientPool
    transaction_repository: minos.aggregate.PostgreSqlTransactionRepository
    event_repository: minos.aggregate.PostgreSqlEventRepository
    snapshot_repository: minos.aggregate.PostgreSqlSnapshotRepository
    saga_manager: minos.saga.SagaManager
    discovery: minos.networks.DiscoveryConnector
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
  - main.FooBarCommandService
rest:
  host: 0.0.0.0
  port: 4546
broker:
  host: localhost
  port: 9092
  queue:
    database: foobar_db
    user: user
    password: pass
    host: localhost
    port: 5432
    records: 1000
    retry: 2
repository:
  database: foobar_db
  user: user
  password: pass
  host: localhost
  port: 5432
snapshot:
  database: foobar_db
  user: user
  password: pass
  host: localhost
  port: 5432
saga:
  storage:
    path: "./foobar.lmdb"
discovery:
  client: minos.networks.InMemoryDiscoveryClient
  host: localhost
  port: 5567
```

</details>

Here is the `foobar/main.py` source file:
<details>
  <summary>Click to show the full file</summary>

```python
from __future__ import annotations

from pathlib import Path
from uuid import UUID

from minos.aggregate import Aggregate, RootEntity
from minos.common import EntrypointLauncher
from minos.cqrs import CommandService
from minos.networks import Request, Response, enroute


class FooBar(RootEntity):
    """FooBar Root Entity clas."""

    something: str


class FooBarAggregate(Aggregate[FooBar]):
    """FooBar Aggregate class."""

    @staticmethod
    async def create_foobar(something: str) -> UUID:
        """Create a new ``FooBar`` instance.
        
        :param something: The something attribute.
        :return: The identifier of the new instance.
        """
        foobar = await FooBar.create(something)
        return foobar.uuid


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

        uuid = await FooBarAggregate.create_foobar(something)

        return Response(uuid)


if __name__ == '__main__':
    launcher = EntrypointLauncher.from_config(Path(__file__).parent / "config.yml")
    launcher.launch()

```

</details>

Execute the following command to start the `foobar` microservice:

```shell
python foobar/main.py
```

To check that everything works fine, execute the following command:

```shell
curl --location --request POST 'http://localhost:4545/foos/add-foobar' \
--header 'Content-Type: application/json' \
--data-raw '{
    "uuid": "YOUR_UUID",
    "something": "something"
}'
```

This request will start a new Saga, that sends a command to the `foobar` microservice, retrieve the `FooBar` identifier and update the `Foo` instance. After that, the `FooQueryService` will handle the update event and print a message similar to this one on the console.

```
The 'foobar' field of a Foo was updated: Event(...)
```

## Packages

This project follows a modular structure based on python packages.

### Core

The core packages provide the base implementation of the framework.

* [minos-microservice-aggregate](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-aggregate): The Aggregate pattern implementation.
* [minos-microservice-common](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-common): The common core package.
* [minos-microservice-cqrs](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-cqrs): The CQRS pattern implementation.
* [minos-microservice-networks](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-networks): The networks core package.
* [minos-microservice-saga](https://minos-framework.github.io/minos-python/packages/core/minos-microservice-saga): The SAGA pattern implementation.

### Plugins

The plugin packages provide connectors to external technologies like brokers, discovery services, databases, serializers and so on.

* [minos-broker-kafka](https://minos-framework.github.io/minos-python/packages/plugins/minos-broker-kafka): The `kafka` plugin package.
* [minos-discovery-minos](https://minos-framework.github.io/minos-python/packages/plugins/minos-discovery-minos): The `minos-discovery` plugin package.

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
