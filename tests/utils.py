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
    AsyncIterator,
    NoReturn,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    Aggregate,
    CommandReply,
    Entity,
    MinosBroker,
    MinosHandler,
    MinosRepository,
    MinosSagaManager,
    MinosSnapshot,
    RepositoryEntry,
    current_datetime,
)

BASE_PATH = Path(__file__).parent


class FakeRepository(MinosRepository):
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.id_counter = 0
        self.items = set()

    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        """For testing purposes."""
        self.id_counter += 1
        entry.id = self.id_counter
        entry.version += 1
        entry.aggregate_uuid = uuid4()
        entry.created_at = current_datetime()
        return entry

    async def _select(self, *args, **kwargs) -> AsyncIterator[RepositoryEntry]:
        """For testing purposes."""


class FakeBroker(MinosBroker):
    """For testing purposes."""

    def __init__(self, **kwargs):
        super().__init__()
        self.call_count = 0
        self.calls_kwargs = list()

    async def send(self, data: Any, **kwargs) -> NoReturn:
        """For testing purposes."""
        self.call_count += 1
        self.calls_kwargs.append({"data": data} | kwargs)

    @property
    def call_kwargs(self) -> Optional[dict[str, Any]]:
        """For testing purposes."""
        if len(self.calls_kwargs) == 0:
            return None
        return self.calls_kwargs[-1]

    def reset_mock(self):
        self.call_count = 0
        self.calls_kwargs = list()


class FakeHandler(MinosHandler):
    """For testing purposes."""

    async def get_one(self, *args, **kwargs) -> Any:
        """For testing purposes."""

    async def get_many(self, *args, **kwargs) -> list[Any]:
        """For testing purposes."""


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    async def _run_new(self, name: str, **kwargs) -> UUID:
        """For testing purposes."""

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        """For testing purposes."""


class FakeEntrypoint:
    """For testing purposes."""

    def __init__(self, *args, **kwargs):
        """For testing purposes."""

    async def __aenter__(self):
        """For testing purposes."""

    async def graceful_shutdown(*args, **kwargs):
        """For testing purposes."""


class FakeLoop:
    """For testing purposes."""

    def __init__(self):
        """For testing purposes."""

    def run_forever(self):
        """For testing purposes."""

    def run_until_complete(self, *args, **kwargs):
        """For testing purposes."""


class FakeSnapshot(MinosSnapshot):
    """For testing purposes."""

    async def get(self, aggregate_name: str, uuids: set[UUID], **kwargs) -> AsyncIterator[Aggregate]:
        """For testing purposes."""


class FakeEntity(Entity):
    """For testing purposes."""

    name: str
