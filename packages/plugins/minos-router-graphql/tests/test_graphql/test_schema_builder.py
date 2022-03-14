import unittest

from minos.plugins.graphql import (
    GraphQlEnroute, GraphQLSchemaBuilder,
)


class TestSomething(unittest.TestCase):

    def test_true(self):
        mutations = [GraphQlCommandEnrouteDecorator('order-command'), GraphQlCommandEnrouteDecorator('ticket-command')]
        queries = [GraphQlQueryEnrouteDecorator('order-query'), GraphQlQueryEnrouteDecorator('ticket-query')]

        schema = GraphQLSchemaBuilder.build(queries=queries, mutations=mutations)
        self.assertTrue(GraphQlEnroute)


if __name__ == '__main__':
    unittest.main()
