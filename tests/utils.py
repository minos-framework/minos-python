"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    NoReturn,
)

from minos.common import (
    CommandReply,
    MinosBroker,
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


def fake_reply(data: MinosModel) -> CommandReply:
    """Fake command reply generator.

    :param data: Data to be set as response on the command reply.
    :return: A Command reply instance.
    """
    return CommandReply("FooCreated", [data], "saga_id")


class NaiveBroker(MinosBroker):
    async def send(self, items: list[MinosModel], **kwargs) -> NoReturn:
        pass
