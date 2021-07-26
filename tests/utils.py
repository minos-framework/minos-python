"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from pathlib import (
    Path,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
    MinosSagaManager,
)
from minos.cqrs import (
    CommandService,
    QueryService,
    Service,
)

BASE_PATH = Path(__file__).parent


class FakeService(Service):
    """For testing purposes."""


class FakeQueryService(QueryService):
    """For testing purposes."""


class FakeCommandService(CommandService):
    """For testing purposes."""


class FakeSagaManager(MinosSagaManager):
    """For testing purposes."""

    async def _run_new(self, name: str, **kwargs) -> UUID:
        """For testing purposes."""

    async def _load_and_run(self, reply: CommandReply, **kwargs) -> UUID:
        """For testing purposes."""
