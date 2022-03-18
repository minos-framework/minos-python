import unittest

from graphql import (
    GraphQLInt,
    GraphQLString,
)

from minos.common import (
    Config,
)
from minos.networks import (
    InMemoryRequest,
    Request,
    Response,
    ResponseException,
)
from minos.plugins.graphql import (
    GraphQlHandler,
    GraphQLSchemaBuilder,
)
from minos.plugins.graphql.decorators import (
    GraphQlCommandEnrouteDecorator,
    GraphQlQueryEnrouteDecorator,
)
from tests.test_graphql.test_builders.test_schema import (
    callback_fn,
)
from tests.utils import (
    BASE_PATH,
    resolve_create_user,
    resolve_user,
)


async def resolve_ticket(request: Request):
    return Response(3)


async def resolve_simple_query(request: Request):
    return Response("ABCD")


async def resolve_ticket_raises(request: Request):
    raise ResponseException("Some error.", status=403)


class TestGraphQlHandler(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = Config(CONFIG_FILE_PATH)

    async def test_execute_operation(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        request = InMemoryRequest(content="{ order_query }")

        result = await handler.execute_operation(request)

        self.assertEqual(200, result.status)
        self.assertDictEqual(await result.content(), {"data": {"order_query": "ticket #4"}, "errors": []})

    async def test_execute_operation_raises(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): resolve_ticket_raises,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        request = InMemoryRequest(content="{ ticket_query }")

        result = await handler.execute_operation(request)

        self.assertEqual(403, result.status)

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

        self.assertEqual(500, result.status)
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

        self.assertEqual(200, result.status)
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

        self.assertEqual(200, result.status)
        self.assertDictEqual({"order_query": 3}, content["data"])
        self.assertEqual([], content["errors"])

    async def test_simple_query(self):
        routes = {GraphQlQueryEnrouteDecorator(name="SimpleQuery", output=GraphQLString): resolve_simple_query}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                            { SimpleQuery }
                            """

        content = {"query": query}

        request = InMemoryRequest(content=content)

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertEqual(200, result.status)
        self.assertDictEqual({"SimpleQuery": "ABCD"}, content["data"])
        self.assertEqual([], content["errors"])

    async def test_query_with_variables_return_user(self):
        routes = {GraphQlQueryEnrouteDecorator("order_query"): resolve_user}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                            query ($userId: String!) {
                                order_query(request: $userId) {
                                    id
                                    firstName
                                    lastName
                                    tweets
                                    verified
                                }
                            }
                            """

        variables = {"userId": "cc44bfe3-7807-4231-8e0d-049d2a6e9ef7"}

        content = {"query": query, "variables": variables}

        request = InMemoryRequest(content=content)

        result = await handler.execute_operation(request)

        content = await result.content()

        self.assertEqual(200, result.status)
        self.assertDictEqual(
            {
                "order_query": {
                    "id": "cc44bfe3-7807-4231-8e0d-049d2a6e9ef7",
                    "firstName": "Jack",
                    "lastName": "Johnson",
                    "tweets": 563,
                    "verified": True,
                }
            },
            content["data"],
        )
        self.assertEqual([], content["errors"])

    async def test_mutation(self):
        routes = {GraphQlCommandEnrouteDecorator("createUser"): resolve_create_user}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        handler = GraphQlHandler(schema)

        query = """
                    mutation ($userData: UserInput!) {
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

        self.assertEqual(200, result.status)
        self.assertDictEqual(
            {
                "createUser": {
                    "id": "cc44bfe3-7807-4231-8e0d-049d2a6e9ef7",
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
