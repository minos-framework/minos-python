from __future__ import (
    annotations,
)

import sys
import unittest
from pathlib import (
    Path,
)
from typing import (
    Any,
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
    Config,
    Lock,
    MinosModel,
    MinosPool,
    SetupMixin,
)
from minos.networks import (
    BrokerClientPool,
    InMemoryBrokerPublisher,
    InMemoryBrokerSubscriberBuilder,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaRequest,
    SagaResponse,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "config.yml"


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.config = Config(BASE_PATH / "config.yml")

        self.broker_publisher = InMemoryBrokerPublisher()
        self.broker_pool = BrokerClientPool.from_config(CONFIG_FILE_PATH)
        self.broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()

        self.lock = FakeLock()
        self.lock_pool = FakeLockPool()

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
        self.container.broker_subscriber_builder = providers.Object(self.broker_subscriber_builder)
        self.container.lock_pool = providers.Object(self.lock_pool)
        self.container.transaction_repository = providers.Object(self.transaction_repository)
        self.container.event_repository = providers.Object(self.event_repository)
        self.container.snapshot_repository = providers.Object(self.snapshot_repository)
        self.container.wire(
            modules=[
                sys.modules["minos.networks"],
                sys.modules["minos.saga"],
                sys.modules["minos.aggregate"],
                sys.modules["minos.common"],
            ]
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


class FakeBrokerPublisher(SetupMixin):
    """For testing purposes."""

    async def send(self, data: Any, **kwargs) -> None:
        """For testing purposes."""


class FakeLock(Lock):
    """For testing purposes."""

    def __init__(self, key=None, *args, **kwargs):
        if key is None:
            key = "fake"
        super().__init__(key, *args, **kwargs)

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        return


class FakeLockPool(MinosPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
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
