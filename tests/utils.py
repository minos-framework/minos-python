"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    namedtuple,
)
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
    MinosSagaManager,
)

BASE_PATH = Path(__file__).parent


class Foo(MinosModel):
    """For testing purposes"""

    text: str


Message = namedtuple("Message", ["topic", "partition", "value"])


class FakeConsumer:
    """For testing purposes."""

    def __init__(self, messages=None):
        if messages is None:
            messages = [Message(topic="TicketAdded", partition=0, value=bytes())]
        self.messages = messages

    async def start(self):
        """For testing purposes."""

    async def stop(self):
        """For testing purposes."""

    async def getmany(self, *args, **kwargs):
        return dict(enumerate(self.messages))

    async def __aiter__(self):
        for message in self.messages:
            yield message


class FakeDispatcher:
    """For testing purposes"""

    def __init__(self):
        self.setup_count = 0
        self.setup_dispatch = 0
        self.setup_destroy = 0

    async def setup(self):
        """For testing purposes."""
        self.setup_count += 1

    async def dispatch(self):
        """For testing purposes."""
        self.setup_dispatch += 1

    async def destroy(self):
        """For testing purposes."""
        self.setup_destroy += 1


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    def __init__(self):
        super().__init__()
        self.name = None
        self.reply = None

    async def _run_new(self, name: str, **kwargs) -> NoReturn:
        self.name = name

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> NoReturn:
        self.reply = reply


class FakeBroker(MinosBroker):
    """For testing purposes."""

    def __init__(self):
        super().__init__()
        self.call_count = 0
        self.items = None
        self.topic = None
        self.saga_uuid = None
        self.reply_topic = None

    async def send(
        self, items: list[MinosModel], topic: str = None, saga_uuid: str = None, reply_topic: str = None, **kwargs
    ) -> NoReturn:
        """For testing purposes."""
        self.call_count += 1
        self.items = items
        self.topic = topic
        self.saga_uuid = saga_uuid
        self.reply_topic = reply_topic
