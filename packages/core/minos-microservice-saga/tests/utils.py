from __future__ import (
    annotations,
)

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
    DeclarativeModel,
    Lock,
    LockPool,
    PoolFactory,
    SetupMixin,
)
from minos.common.testing import (
    MinosTestCase,
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
    testing,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "config.yml"
DB_PATH = BASE_PATH / "test_db.lmdb"


class SagaTestCase(MinosTestCase):
    testing_module = testing

    def get_config_file_path(self):
        return CONFIG_FILE_PATH

    def get_injections(self):
        pool_factory = PoolFactory.from_config(
            self.config, default_classes={"broker": BrokerClientPool, "lock": FakeLockPool}
        )
        broker_publisher = InMemoryBrokerPublisher()
        broker_subscriber_builder = InMemoryBrokerSubscriberBuilder()
        transaction_repository = InMemoryTransactionRepository(lock_pool=pool_factory.get_pool("lock"))
        event_repository = InMemoryEventRepository(
            broker_publisher=broker_publisher,
            transaction_repository=transaction_repository,
            lock_pool=pool_factory.get_pool("lock"),
        )
        snapshot_repository = InMemorySnapshotRepository(
            event_repository=event_repository, transaction_repository=transaction_repository
        )
        return [
            pool_factory,
            broker_publisher,
            broker_subscriber_builder,
            transaction_repository,
            event_repository,
            snapshot_repository,
        ]


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

    async def acquire(self) -> None:
        """For testing purposes."""

    async def release(self):
        """For testing purposes."""


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
