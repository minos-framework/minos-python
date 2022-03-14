from functools import (
    total_ordering,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
)

from graphql import GraphQLObjectType, GraphQLField, GraphQLString, GraphQLArgument
from minos.common import (
    DeclarativeModel,
)
from minos.networks import (
    Request,
    Response,
    enroute,
)

BASE_PATH = Path(__file__).parent
CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"


@total_ordering
class FakeModel(DeclarativeModel):
    """For testing purposes"""

    data: Any

    def __lt__(self, other: Any) -> bool:
        # noinspection PyBroadException
        return isinstance(other, type(self)) and self.data < other.data


class FakeCommandService:
    """For testng purposes."""

    #@enroute.broker.command("GetOrder")
    def get_order_command(self, request: Request) -> Response:
        """For testng purposes."""
        return Response("get_order_command")
    """
    @enroute.graphql.command(
        args={"request": GraphQLArgument(GraphQlObject({"name": GraphQLString, "surname": GraphQLString}))},
        response=GrapqlUUID
    )
    """
    def get_hero(self, request: Request) -> Response:
        """For testng purposes."""
        uuid = request.content()
        return Response(uuid)



class FakeQueryService:
    """For testng purposes."""

    # noinspection PyUnusedLocal
    @enroute.graphql.query(name="order", argument=GraphQLField(GraphQLString), output=GraphQLString)
    def get_order(self, request: Request):
        """For testng purposes."""

        return "eu38hj32-889283-j2jjb5kl"

