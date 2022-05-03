import unittest

from graphql import (
    GraphQLField,
    GraphQLString,
)

from minos.common import (
    classname,
)
from minos.networks import (
    EnrouteCollector,
)
from minos.plugins.graphql import (
    GraphQlQueryEnrouteDecorator,
)
from tests.utils import (
    FakeQueryService,
    FakeQueryService2,
)


class TestGraphQlEnrouteDecorator(unittest.TestCase):
    def test_decorated_str(self):
        analyzer = EnrouteCollector(classname(FakeQueryService))
        self.assertEqual(FakeQueryService, analyzer.decorated)

    def test_get_all_queries(self):
        analyzer = EnrouteCollector(FakeQueryService)
        observed = analyzer.get_all()

        expected = {
            "get_order": {
                GraphQlQueryEnrouteDecorator(name="order", argument=GraphQLField(GraphQLString), output=GraphQLString)
            },
        }

        self.assertEqual(expected, observed)

    def test_get_all_queries_with_kwargs(self):
        analyzer = EnrouteCollector(FakeQueryService2)
        observed = analyzer.get_all()

        expected = {
            "get_order": {
                GraphQlQueryEnrouteDecorator(
                    name="order",
                    argument=GraphQLField(GraphQLString),
                    output=GraphQLString,
                    foo="bar",
                )
            },
        }

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
