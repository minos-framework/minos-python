from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

from minos.aggregate import (
    Aggregate,
    AggregateRef,
    ModelRef,
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
    async def find_foo(self, request: Request) -> Response:
        """For testing purpose"""


class FakeCommandService(CommandService):
    """For testing purposes."""

    @enroute.broker.command("CreateFoo")
    async def create_foo(self, request: Request) -> Response:
        """For testing purpose"""


class Foo(Aggregate):
    """For testing purposes"""

    bar: ModelRef[Bar]


class Bar(AggregateRef):
    """For testing purposes"""

    name: str
