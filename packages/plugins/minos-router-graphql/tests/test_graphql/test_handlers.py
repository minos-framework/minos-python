import unittest

from graphql import (
    GraphQLInt,
    GraphQLNonNull,
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
    GraphQlCommandEnrouteDecorator,
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
    resolve_create_user,
    resolve_user,
    user_input_type,
    user_type,
)


async def resolve_ticket(request: Request):
    return Response(3)


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

        self.assertNotEqual(content["errors"], [])

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

    async def test_query_with_variables(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLInt, output=GraphQLInt): resolve_ticket
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                            query ($userId: Int!) {
                                order_query(request: $userId)
                            }
                            """

        variables = {"userId": 3}

        content = {"query": query, "variables": variables}

        request = InMemoryRequest(content=content)

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertDictEqual({"order_query": 3}, content["data"])
        self.assertEqual([], content["errors"])

    async def test_query_with_variables_return_user(self):
        routes = {GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLInt, output=user_type): resolve_user}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                            query ($userId: Int!) {
                                order_query(request: $userId) {
                                    id
                                    firstName
                                    lastName
                                    tweets
                                    verified
                                }
                            }
                            """

        variables = {"userId": 3}

        content = {"query": query, "variables": variables}

        request = InMemoryRequest(content=content)

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertDictEqual(
            {"order_query": {"id": "3", "firstName": "Jack", "lastName": "Johnson", "tweets": 563, "verified": True}},
            content["data"],
        )
        self.assertEqual([], content["errors"])

    async def test_mutation(self):
        routes = {
            GraphQlCommandEnrouteDecorator(
                name="createUser", argument=GraphQLNonNull(user_input_type), output=user_type
            ): resolve_create_user
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                    mutation ($userData: UserInputType!) {
                        createUser(request: $userData) {
                            id, firstName, lastName, tweets, verified
                        }
                    }
                    """

        variables = {"userData": dict(firstName="John", lastName="Doe", tweets=42, verified=True)}

        content = {"query": query, "variables": variables}

        request = InMemoryRequest(content=content)

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertDictEqual(
            {
                "createUser": {
                    "id": "4kjjj43-l23k4l3-325kgaa2",
                    "firstName": "John",
                    "lastName": "Doe",
                    "tweets": 42,
                    "verified": True,
                }
            },
            content["data"],
        )
        self.assertEqual([], content["errors"])


if __name__ == "__main__":
    unittest.main()
