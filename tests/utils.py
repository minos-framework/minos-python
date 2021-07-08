"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from collections import (
    namedtuple,
)
from datetime import (
    datetime,
)
from pathlib import (
    Path,
)
from typing import (
    AsyncIterator,
    NoReturn,
)

from minos.common import (
    AggregateDiff,
    CommandReply,
    CommandStatus,
    Field,
    FieldsDiff,
    MinosBroker,
    MinosModel,
    MinosRepository,
    MinosSagaManager,
    RepositoryEntry,
)

BASE_PATH = Path(__file__).parent

FAKE_AGGREGATE_DIFF = AggregateDiff(1, "Foo", 3, FieldsDiff({"doors": Field("doors", int, 5)}))


class FakeModel(MinosModel):
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
        self.status = None

    async def send(
        self,
        items: list[MinosModel],
        topic: str = None,
        saga_uuid: str = None,
        reply_topic: str = None,
        status: CommandStatus = None,
        **kwargs
    ) -> NoReturn:
        """For testing purposes."""
        self.call_count += 1
        self.items = items
        self.topic = topic
        self.saga_uuid = saga_uuid
        self.reply_topic = reply_topic
        self.status = status


class FakeRepository(MinosRepository):
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id_counter = 0
        self.items = list()

    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        """For testing purposes."""
        self.id_counter += 1
        self.items.append(entry)
        entry.id = self.id_counter
        entry.version += 1
        entry.aggregate_id = 9999
        entry.created_at = datetime.now()

        return entry

    async def _select(self, *args, **kwargs) -> AsyncIterator[RepositoryEntry]:
        """For testing purposes."""
        for item in self.items:
            yield item
