from __future__ import (
    annotations,
)

import sys
import typing as t
import unittest
from pathlib import (
    Path,
)
from typing import (
    Any,
)

from aiomisc.pool import (
    T,
)
from dependency_injector import (
    containers,
    providers,
)

from minos.aggregate import (
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
)
from minos.common import (
    Lock,
    MinosConfig,
    MinosModel,
    MinosPool,
    MinosSetup,
)
from minos.networks import (
    REPLY_TOPIC_CONTEXT_VAR,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaRequest,
    SagaResponse,
)

BASE_PATH = Path(__file__).parent


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.config = MinosConfig(BASE_PATH / "config.yml")

        self.broker_publisher = FakeBrokerPublisher()
        self.broker = FakeBroker("TheReplyTopic", self.broker_publisher)
        self.broker_pool = FakePool(self.broker)

        self.lock = FakeLock()
        self.lock_pool = FakePool(self.lock)

        self.transaction_repository = InMemoryTransactionRepository(lock_pool=self.lock_pool)
        self.event_repository = InMemoryEventRepository(
            broker_publisher=self.broker_publisher,
            transaction_repository=self.transaction_repository,
            lock_pool=self.lock_pool,
        )
        self.snapshot_repository = InMemorySnapshotRepository(
            event_repository=self.event_repository, transaction_repository=self.transaction_repository
        )

        self.container = containers.DynamicContainer()
        self.container.config = providers.Object(self.config)
        self.container.broker_pool = providers.Object(self.broker_pool)
        self.container.broker_publisher = providers.Object(self.broker_publisher)
        self.container.lock_pool = providers.Object(self.lock_pool)
        self.container.transaction_repository = providers.Object(self.transaction_repository)
        self.container.event_repository = providers.Object(self.event_repository)
        self.container.snapshot_repository = providers.Object(self.snapshot_repository)
        self.container.wire(
            modules=[sys.modules["minos.saga"], sys.modules["minos.aggregate"], sys.modules["minos.common"]]
        )

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.broker_publisher.setup()
        await self.transaction_repository.setup()
        await self.lock_pool.setup()
        await self.event_repository.setup()
        await self.snapshot_repository.setup()

    async def asyncTearDown(self):
        await self.snapshot_repository.destroy()
        await self.event_repository.destroy()
        await self.lock_pool.destroy()
        await self.transaction_repository.destroy()
        await self.broker_publisher.destroy()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.container.unwire()
        super().tearDown()


class FakeBrokerPublisher(MinosSetup):
    """For testing purposes."""

    async def send(self, data: Any, **kwargs) -> None:
        """For testing purposes."""


class FakeLock(Lock):
    """For testing purposes."""

    def __init__(self, key=None, *args, **kwargs):
        if key is None:
            key = "fake"
        super().__init__(key, *args, **kwargs)

    async def __aenter__(self):
        """For testing purposes."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """For testing purposes."""


class FakeBroker:
    """For testing purposes."""

    def __init__(self, topic, publisher):
        super().__init__()
        self.topic = topic
        self.publisher = publisher
        self._token = None

    async def send(self, *args, **kwargs) -> None:
        """For testing purposes."""
        await self.publisher.send(*args, reply_topic=self.topic, **kwargs)

    async def get_many(self, count: int, *args, **kwargs) -> list[Any]:
        """For testing purposes."""
        return [await self.get_one(*args, **kwargs) for _ in range(count)]

    async def get_one(self, *args, **kwargs) -> Any:
        """For testing purposes."""

    async def __aenter__(self):
        """For testing purposes."""
        if self._token is None:
            self._token = REPLY_TOPIC_CONTEXT_VAR.set(self.topic)
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """For testing purposes."""
        if self._token is not None:
            REPLY_TOPIC_CONTEXT_VAR.reset(self._token)
            self._token = None


class FakePool(MinosPool):
    """For testing purposes."""

    def __init__(self, instance):
        super().__init__()
        self.instance = instance

    def acquire(self, *args, **kwargs):
        """For testing purposes."""
        return self.instance

    async def _create_instance(self) -> T:
        """For testing purposes."""

    async def _destroy_instance(self, instance: t.Any) -> None:
        """For testing purposes."""


class Foo(MinosModel):
    """Utility minos model class for testing purposes"""

    foo: str


# noinspection PyUnusedLocal
async def send_create_product(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("CreateProduct", context["product"])


async def handle_product_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["product"] = await response.content()
    return context


# noinspection PyUnusedLocal
async def send_create_ticket(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("CreateTicket", Foo("create_ticket!"))


# noinspection PyUnusedLocal
async def send_create_ticket_raises(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    raise ValueError()


# noinspection PyUnusedLocal
async def send_delete_ticket(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("CreateTicket", Foo("delete_ticket!"))


# noinspection PyUnusedLocal
async def send_create_order(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("CreateOrder", Foo("create_order!"))


# noinspection PyUnusedLocal
async def send_delete_order(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("DeleteOrder", Foo("delete_order!"))


async def handle_order_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["order"] = await response.content()
    return context


# noinspection PyUnusedLocal
async def handle_ticket_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["ticket"] = await response.content()
    return context


# noinspection PyUnusedLocal
async def handle_ticket_success_raises(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    raise ValueError()


# noinspection PyUnusedLocal
async def handle_ticket_error(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["ticket"] = None
    return context


# noinspection PyUnusedLocal
async def handle_ticket_error_raises(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    raise ValueError()


def create_payment(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    context["payment"] = "payment"
    return context


# noinspection PyUnusedLocal
async def create_payment_raises(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    raise ValueError()


def delete_payment(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    context["payment"] = None
    return context


# fmt: off
ADD_ORDER = (
    Saga()
        .remote_step(send_create_order)
        .on_success(handle_order_success)
        .on_failure(send_delete_order)
        .local_step(create_payment)
        .on_failure(delete_payment)
        .remote_step(send_create_ticket)
        .on_success(handle_ticket_success)
        .on_error(handle_ticket_error)
        .on_failure(send_delete_ticket)
        .commit()
)

# fmt: off
DELETE_ORDER = (
    Saga()
        .remote_step(send_delete_order)
        .on_success(handle_order_success)
        .remote_step(send_delete_ticket)
        .on_success(handle_ticket_success_raises)
        .commit()
)

# fmt: off
CREATE_PAYMENT = (
    Saga()
        .local_step(create_payment)
        .on_failure(delete_payment)
        .commit()
)


def add_order_condition(context: SagaContext) -> bool:
    """For testing purposes."""
    return "a" in context


def delete_order_condition(context: SagaContext) -> bool:
    """For testing purposes."""
    return "b" in context
