from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)
from typing import (
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from cached_property import (
    cached_property,
)

from minos.aggregate import (
    Aggregate,
    AggregateRef,
    ModelRef,
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
from minos.networks import (
    Request,
    Response,
    enroute,
)

BASE_PATH = Path(__file__).parent


class FakeService(Service):
    """For testing purposes."""


class FakeQueryService(QueryService):
    """For testing purposes."""

    @enroute.broker.query("FindFoo")
    def find_foo(self, request: Request) -> Response:
        """For testing purpose"""


class FakeCommandService(CommandService):
    """For testing purposes."""

    @enroute.broker.command("CreateFoo")
    def create_foo(self, request: Request) -> Response:
        """For testing purpose"""


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

    @cached_property
    def user(self) -> Optional[UUID]:
        return uuid4()

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
