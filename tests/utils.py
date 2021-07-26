"""
Copyright (C) 2021 Clariteia SL
This file is part of minos framework.
Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from uuid import (
    UUID,
)

from minos.common import (
    Aggregate,
    AggregateRef,
    CommandReply,
    MinosSagaManager,
    ModelRef,
)
from minos.cqrs import (
    CommandService,
    QueryService,
    Service,
)
from minos.networks import (
    Request,
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


class FakeRequest(Request):
    """For testing purposes"""

    def __init__(self, content):
        super().__init__()
        self._content = content

    async def content(self, **kwargs):
        """For testing purposes"""
        return self._content

    def __eq__(self, other) -> bool:
        return self._content == other._content

    def __repr__(self) -> str:
        return f"FakeRequest({self._content!r})"


class Foo(Aggregate):
    """For testing purposes"""

    bar: ModelRef[Bar]


class Bar(AggregateRef):
    """For testing purposes"""

    name: str
