from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from typing import (
    Any,
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
    ConditionalSagaStep,
    ElseThenAlternative,
    IfThenAlternative,
    LocalSagaStep,
    RemoteSagaStep,
    Saga,
    SagaContext,
    SagaRequest,
    SagaResponse,
    testing,
)
from minos.transactions import (
    InMemoryTransactionRepository,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "config.yml"


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
        return [
            pool_factory,
            broker_publisher,
            broker_subscriber_builder,
            transaction_repository,
        ]


class FakeBrokerPublisher(SetupMixin):
    """For testing purposes."""

    async def send(self, data: Any, **kwargs) -> None:
        """For testing purposes."""


class FakeAsyncIterator:
    """For testing purposes."""

    def __init__(self, seq):
        self.iter = iter(seq)

    def __aiter__(self):
        return self

    async def __anext__(self):
        try:
            return next(self.iter)
        except StopIteration:
            raise StopAsyncIteration


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
async def send_create_order(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("CreateOrder", Foo("create_order!"))


# noinspection PyUnusedLocal
async def handle_ticket_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["ticket"] = await response.content()
    return context


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


@Saga()
class DeleteOrderSaga:
    """For testing purposes"""

    @LocalSagaStep(order=1)
    def _something(self, context: SagaContext) -> SagaContext:
        return context

    # noinspection PyUnusedLocal
    @RemoteSagaStep(order=2)
    async def send_delete_order(self, context: SagaContext) -> SagaRequest:
        """For testing purposes."""
        return SagaRequest("DeleteOrder", Foo("delete_order!"))

    @send_delete_order.on_success()
    async def handle_order_success(self, context: SagaContext, response: SagaResponse) -> SagaContext:
        """For testing purposes."""
        context["order"] = await response.content()
        return context

    # noinspection PyUnusedLocal
    @RemoteSagaStep(order=3)
    async def send_delete_ticket(self, context: SagaContext) -> SagaRequest:
        """For testing purposes."""
        return SagaRequest("CreateTicket", Foo("delete_ticket!"))

    # noinspection PyUnusedLocal
    @send_delete_ticket.on_success()
    async def handle_ticket_success_raises(self, context: SagaContext, response: SagaResponse) -> SagaContext:
        """For testing purposes."""
        raise ValueError()


# fmt: off
ADD_ORDER = (
    Saga()
        .remote_step(send_create_order)
        .on_success(DeleteOrderSaga.handle_order_success)
        .on_failure(DeleteOrderSaga.send_delete_order)
        .local_step(create_payment)
        .on_failure(delete_payment)
        .remote_step(send_create_ticket)
        .on_success(handle_ticket_success)
        .on_error(handle_ticket_error)
        .on_failure(DeleteOrderSaga.send_delete_ticket)
        .commit()
)

# fmt: off
DELETE_ORDER = (
    Saga()
        .remote_step(DeleteOrderSaga.send_delete_order)
        .on_success(DeleteOrderSaga.handle_order_success)
        .remote_step(DeleteOrderSaga.send_delete_ticket)
        .on_success(DeleteOrderSaga.handle_ticket_success_raises)
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
