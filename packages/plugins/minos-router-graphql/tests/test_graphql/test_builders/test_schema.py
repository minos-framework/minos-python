import unittest

from graphql import (
    GraphQLID,
    GraphQLString,
    graphql,
    validate_schema,
)

from minos.common import (
    Config,
)
from minos.networks import (
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
from tests.utils import (
    BASE_PATH,
    user_type,
)


async def callback_fn(request: Request):
    return Response("ticket #4")


class TestGraphQLSchemaBuilder(unittest.IsolatedAsyncioTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = Config(CONFIG_FILE_PATH)

    def test_build(self):
        routes = {
            GraphQlCommandEnrouteDecorator(
                name="order_command", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
            GraphQlCommandEnrouteDecorator(
                name="ticket_command", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLString, output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(
                name="ticket_query", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)

    def test_build_only_queries(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLString, output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(
                name="ticket_query", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)

    def test_build_queries_without_arguments(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(name="ticket_query", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)

    def test_build_only_commands(self):
        routes = {
            GraphQlCommandEnrouteDecorator(
                name="order_command", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
            GraphQlCommandEnrouteDecorator(
                name="ticket_command", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)

    def test_build_commands_without_arguments(self):
        routes = {
            GraphQlCommandEnrouteDecorator(name="order_command", output=GraphQLString): callback_fn,
            GraphQlCommandEnrouteDecorator(name="ticket_command", output=GraphQLString): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)

    async def test_query(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLString, output=GraphQLString): callback_fn
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        source = "{ order_query }"

        result = await graphql(schema, source)

        self.assertEqual({"order_query": "ticket #4"}, result.data)
        self.assertEqual(None, result.errors)

    async def test_query_without_arguments(self):
        routes = {GraphQlQueryEnrouteDecorator(name="order_query", output=GraphQLString): callback_fn}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        source = "{ order_query }"

        result = await graphql(schema, source)

        self.assertEqual({"order_query": "ticket #4"}, result.data)
        self.assertEqual(None, result.errors)

    async def test_schema(self):
        routes = {GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLID, output=user_type): callback_fn}

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(list(), errors)


if __name__ == "__main__":
    unittest.main()
