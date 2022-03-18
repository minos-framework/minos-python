from pathlib import (
    Path,
)
from uuid import (
    UUID,
)

from graphql import (
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


class FakeQueryService:
    """For testng purposes."""

    # noinspection PyUnusedLocal
    @enroute.graphql.query(name="order", argument=GraphQLString, output=GraphQLString)
    def get_order(self, request: Request):
        """For testng purposes."""

        return "eu38hj32-889283-j2jjb5kl"


class User(DeclarativeModel):
    """A simple user object class."""

    id: UUID
    firstName: str
    lastName: str
    tweets: int
    verified: bool


class UserInput(DeclarativeModel):
    """A simple user object class."""

    firstName: str
    lastName: str
    tweets: int
    verified: bool


async def resolve_user(request: Request[UUID]) -> Response[User]:
    uuid = await request.content()
    return Response(User(uuid, firstName="Jack", lastName="Johnson", tweets=563, verified=True))


async def resolve_create_user(request: Request[UserInput]) -> Response[User]:
    params = await request.content()
    return Response(
        User(
            firstName=params["firstName"],
            lastName=params["lastName"],
            tweets=params["tweets"],
            id="cc44bfe3-7807-4231-8e0d-049d2a6e9ef7",
            verified=params["verified"],
        )
    )
