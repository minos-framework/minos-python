from __future__ import (
    annotations,
)

from pathlib import (
    Path,
)

from minos.aggregate import (
    Entity,
    Ref,
)
from minos.common import (
    Injectable,
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


@Injectable("aggregate")
class FakeAggregate:
    """For testing purposes."""


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


class Foo(Entity):
    """For testing purposes"""

    bar: Ref["Bar"]
