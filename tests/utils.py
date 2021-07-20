"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    Any,
    NoReturn,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    CommandReply,
    CommandStatus,
    MinosBroker,
    MinosHandler,
    MinosModel,
)
from minos.saga import (
    SagaContext,
)

BASE_PATH = Path(__file__).parent


class Foo(MinosModel):
    """Utility minos model class for testing purposes"""

    foo: str


# noinspection PyUnusedLocal
def foo_fn(context: SagaContext) -> MinosModel:
    """Utility callback function for testing purposes.

    :param context: A context instance.
    :return: A minos model function.
    """
    return Foo("hello")


# noinspection PyUnusedLocal
def foo_fn_raises(context: SagaContext) -> MinosModel:
    """Utility callback function for testing purposes that raises an exception.

    :param context: A context instance.
    :return: A minos model function.
    """
    raise ValueError()


def fake_reply(
    data: Any = None, uuid: Optional[UUID] = None, status: CommandStatus = CommandStatus.SUCCESS
) -> CommandReply:
    """For testing purposes."""

    if uuid is None:
        uuid = uuid4()
    return CommandReply("FooCreated", data, uuid, status=status)


class NaiveBroker(MinosBroker):
    """For testing purposes."""

    async def send(self, data: Any, **kwargs) -> NoReturn:
        """For testing purposes."""


class FakeHandler(MinosHandler):
    """For testing purposes."""

    async def get_one(self, *args, **kwargs) -> Any:
        """For testing purposes."""

    async def get_many(self, *args, **kwargs) -> list[Any]:
        """For testing purposes."""
