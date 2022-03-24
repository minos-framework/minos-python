from __future__ import (
    annotations,
)

import unittest
from pathlib import (
    Path,
)
from typing import (
    Any,
)

from minos.aggregate import (
    InMemoryEventRepository,
    InMemorySnapshotRepository,
    InMemoryTransactionRepository,
)
from minos.common import (
    Config,
    DeclarativeModel,
    DependencyInjector,
    Lock,
    LockPool,
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
DB_PATH = BASE_PATH / "test_db.lmdb"


class MinosTestCase(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.config = Config(CONFIG_FILE_PATH)

        self.broker_pool = BrokerClientPool.from_config(CONFIG_FILE_PATH)
        self.broker_publisher = InMemoryBrokerPublisher()
        self.broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()
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

        self.injector = DependencyInjector(
            self.config,
            [
                self.broker_pool,
                self.broker_publisher,
                self.broker_subscriber_builder,
                self.lock_pool,
                self.transaction_repository,
                self.event_repository,
                self.snapshot_repository,
            ],
        )
        self.injector.wire_injections()

    async def asyncSetUp(self):
        await super().asyncSetUp()

        await self.injector.setup_injections()

    async def asyncTearDown(self):
        await self.injector.destroy_injections()

        await super().asyncTearDown()

    def tearDown(self) -> None:
        self.injector.unwire_injections()
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


class FakeLockPool(LockPool):
    """For testing purposes."""

    async def _create_instance(self):
        return FakeLock()

    async def _destroy_instance(self, instance) -> None:
        """For testing purposes."""


class Foo(DeclarativeModel):
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
