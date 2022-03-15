import json
import unittest
from typing import (
    NamedTuple,
    Optional,
)

from graphql import (
    GraphQLArgument,
    GraphQLBoolean,
    GraphQLField,
    GraphQLID,
    GraphQLInt,
    GraphQLNonNull,
    GraphQLObjectType,
    GraphQLString,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    InMemoryRequest,
    Request,
    Response,
)
from minos.plugins.graphql import (
    GraphQLSchemaBuilder,
)
from minos.plugins.graphql.decorators import (
    GraphQlQueryEnrouteDecorator,
)
from minos.plugins.graphql.handlers import (
    GraphQlHandler,
)
from tests.test_graphql.test_builders.test_schema import (
    callback_fn,
)
from tests.utils import (
    BASE_PATH,
)


class User(NamedTuple):
    """A simple user object class."""

    firstName: str
    lastName: str
    tweets: Optional[int]
    id: Optional[str] = None
    verified: bool = False


async def resolve_ticket(request: Request):
    return Response(3)


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


class TestGraphQlHttpRouter(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = MinosConfig(CONFIG_FILE_PATH)

    async def test_execute_operation(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        request = InMemoryRequest(content="{ order_query }")

        result = await handler.execute_operation(request)

        self.assertDictEqual(await result.content(), {"data": {"order_query": "ticket #4"}, "errors": []})

    async def test_execute_wrong_operation(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        request = InMemoryRequest(content="{ fictitious_query }")

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertNotEquals(content["errors"], [])

    async def test_schema(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        request = InMemoryRequest(content="Some content")

        result = await handler.get_schema(request)

        self.assertIsInstance(await result.content(), str)
        self.assertMultiLineEqual(
            await result.content(), "type Query {\n  order_query: String\n  ticket_query: String\n}"
        )

    async def operation_with_variables(self):
        routes = {
            GraphQlQueryEnrouteDecorator(
                name="order_query", argument=GraphQLArgument(GraphQLID), output=user_type
            ): callback_fn
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                            query ($userId: ID!) {
                                User(id: $userId) {
                                    id, firstName, lastName, tweets, verified
                                }
                            }
                            """

        variables = {"userId": 3}

        content = {"query": query, "variables": variables}

        request = InMemoryRequest(content=json.dumps(content))

        result = await handler.get_schema(request)

        self.assertIsInstance(await result.content(), str)
        self.assertMultiLineEqual(
            await result.content(), "type Query {\n  order_query: String\n  ticket_query: String\n}"
        )


if __name__ == "__main__":
    unittest.main()
