from functools import (
    total_ordering,
)
from pathlib import (
    Path,
)
from typing import (
    Any,
    NamedTuple,
    Optional,
)

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLField,
    GraphQLID,
    GraphQLInputField,
    GraphQLInputObjectType,
    GraphQLInt,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
)

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

    # @enroute.broker.command("GetOrder")
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


class FakeQueryService2:
    """For testng purposes."""

    # noinspection PyUnusedLocal
    @enroute.graphql.query(name="order", argument=GraphQLField(GraphQLString), output=GraphQLString, authorized=True,
                           allowed_groups=['supèr_admin', 'admin', ])
    def get_order(self, request: Request):
        """For testng purposes."""

        return "eu38hj32-889283-j2jjb5kl"


user_type = GraphQLObjectType(
    "UserType",
    {
        "id": GraphQLField(GraphQLNonNull(GraphQLID)),
        "firstName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "lastName": GraphQLField(GraphQLNonNull(GraphQLString)),
        "tweets": GraphQLField(GraphQLInt),
        "verified": GraphQLField(GraphQLNonNull(GraphQLBoolean)),
    },
)

user_input_type = GraphQLInputObjectType(
    "UserInputType",
    {
        "firstName": GraphQLInputField(GraphQLNonNull(GraphQLString)),
        "lastName": GraphQLInputField(GraphQLNonNull(GraphQLString)),
        "tweets": GraphQLInputField(GraphQLInt),
        "verified": GraphQLInputField(GraphQLBoolean),
    },
)


class User(NamedTuple):
    """A simple user object class."""

    firstName: str
    lastName: str
    tweets: Optional[int]
    id: Optional[str] = None
    verified: bool = False


async def resolve_user(request: Request):
    id = await request.content()
    return Response(User(firstName="Jack", lastName="Johnson", tweets=563, id=str(id), verified=True))


async def resolve_create_user(request: Request):
    params = await request.content()
    return Response(
        User(
            firstName=params["firstName"],
            lastName=params["lastName"],
            tweets=params["tweets"],
            id="4kjjj43-l23k4l3-325kgaa2",
            verified=params["verified"],
        )
    )
