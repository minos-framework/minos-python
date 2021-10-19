from __future__ import (
    annotations,
)

import typing as t
from pathlib import (
    Path,
)
from typing import (
    Any,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from aiomisc.pool import (
    T,
)

from minos.common import (
    CommandReply,
    CommandStatus,
    MinosBroker,
    MinosHandler,
    MinosModel,
    MinosPool,
)
from minos.saga import (
    Saga,
    SagaContext,
    SagaRequest,
    SagaResponse,
)

BASE_PATH = Path(__file__).parent


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


def commit_callback(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    context["status"] = "Finished!"
    return context


# noinspection PyUnusedLocal
def commit_callback_raises(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    raise ValueError()


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


class NaiveBroker(MinosBroker):
    """For testing purposes."""

    async def send(self, data: Any, **kwargs) -> None:
        """For testing purposes."""


class FakeHandler(MinosHandler):
    """For testing purposes."""

    def __init__(self, topic):
        super().__init__()
        self.topic = topic

    async def get_one(self, *args, **kwargs) -> Any:
        """For testing purposes."""

    async def get_many(self, *args, **kwargs) -> list[Any]:
        """For testing purposes."""


class FakePool(MinosPool):
    """For testing purposes."""

    def __init__(self, instance):
        super().__init__()
        self.instance = instance

    async def _create_instance(self) -> T:
        """For testing purposes."""
        return self.instance

    async def _destroy_instance(self, instance: t.Any) -> None:
        """For testing purposes."""


def fake_reply(data: Any = None, uuid: Optional[UUID] = None, status: Optional[CommandStatus] = None) -> CommandReply:
    """For testing purposes."""

    if uuid is None:
        uuid = uuid4()
    if status is None:
        status = CommandStatus.SUCCESS
    return CommandReply("FooCreated", data, uuid, status=status)
