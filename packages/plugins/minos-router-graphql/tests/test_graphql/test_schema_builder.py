import unittest

from graphql import (
    GraphQLString,
    graphql_sync,
    validate_schema,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    Request,
    Response,
)
from minos.plugins.graphql import (
    GraphQlEnroute,
    GraphQlHttpRouter,
    GraphQLSchemaBuilder,
)
from minos.plugins.graphql.decorators import (
    GraphQlCommandEnrouteDecorator,
    GraphQlQueryEnrouteDecorator,
)
from tests.utils import (
    BASE_PATH,
)


async def callback_fn(request: Request):
    return Response("ticket #4")


class TestSomething(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = MinosConfig(CONFIG_FILE_PATH)

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

        self.assertEqual(errors, [])

    def test_build_only_queries(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLString, output=GraphQLString): callback_fn,
            GraphQlQueryEnrouteDecorator(
                name="ticket_query", argument=GraphQLString, output=GraphQLString
            ): callback_fn,
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        errors = validate_schema(schema)

        self.assertEqual(errors, [])

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

        self.assertEqual(errors, [])

    def test_schema_valid(self):
        router = GraphQlHttpRouter.from_config(config=self._config)

        errors = validate_schema(router._schema)

        self.assertEqual(errors, [])

    def test_query(self):
        routes = {
            GraphQlQueryEnrouteDecorator(name="order_query", argument=GraphQLString, output=GraphQLString): callback_fn
        }

        schema = GraphQLSchemaBuilder.build(routes=routes)

        source = "{ order_query }"

        result = graphql_sync(schema, source)

        self.assertEqual(result.errors, [])


if __name__ == "__main__":
    unittest.main()
