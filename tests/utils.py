"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from typing import (
    AsyncIterator,
    NoReturn,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
    MinosBroker,
    MinosModel,
    MinosRepository,
    MinosRepositoryEntry,
    MinosSagaManager,
)

BASE_PATH = Path(__file__).parent


class FakeRepository(MinosRepository):
    """For testing purposes."""

    async def _submit(self, entry: MinosRepositoryEntry) -> MinosRepositoryEntry:
        """For testing purposes."""

    async def _select(self, *args, **kwargs) -> AsyncIterator[MinosRepositoryEntry]:
        """For testing purposes."""


class FakeBroker(MinosBroker):
    """For testing purposes."""

    @classmethod
    async def send(cls, items: list[MinosModel], **kwargs) -> NoReturn:
        """For testing purposes."""


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    async def _run_new(self, name: str, **kwargs) -> UUID:
        """For testing purposes."""

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        """For testing purposes."""
