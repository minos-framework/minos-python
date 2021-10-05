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
    return SagaRequest("CreateProduct", Foo("create_product!"))


# noinspection PyUnusedLocal
async def send_delete_product(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("DeleteProduct", Foo("delete_product!"))


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
    return SagaRequest("CreateOrder", Foo("create_order"))


# noinspection PyUnusedLocal
async def send_delete_order(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("DeleteOrder", Foo("delete_order!"))


# noinspection PyUnusedLocal
async def send_verify_consumer(context: SagaContext) -> SagaRequest:
    """For testing purposes."""
    return SagaRequest("VerifyConsumer", Foo("verify_consumer!"))


async def handle_order_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["order"] = await response.content()
    return context


async def handle_product_success(context: SagaContext, response: SagaResponse) -> SagaContext:
    """For testing purposes."""
    context["product"] = await response.content()
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


def commit_callback(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    context["status"] = "Finished!"
    return context


# noinspection PyUnusedLocal
def commit_callback_raises(context: SagaContext) -> SagaContext:
    """For testing purposes."""
    raise ValueError()


ADD_ORDER = (
    Saga()
    .step()
    .invoke_participant(send_create_product)
    .with_compensation(send_delete_product)
    .on_reply(handle_product_success)
    .step()
    .invoke_participant(send_create_ticket)
    .with_compensation(send_delete_order)
    .on_reply(handle_ticket_success)
    .commit()
)

DELETE_ORDER = (
    Saga()
    .step()
    .invoke_participant(send_delete_product)
    .on_reply(handle_product_success)
    .step()
    .invoke_participant(send_delete_ticket)
    .on_reply(handle_ticket_success_raises)
    .commit()
)


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


def fake_reply(
    data: Any = None, uuid: Optional[UUID] = None, status: CommandStatus = CommandStatus.SUCCESS
) -> CommandReply:
    """For testing purposes."""

    if uuid is None:
        uuid = uuid4()
    return CommandReply("FooCreated", data, uuid, status=status)
