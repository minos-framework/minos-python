import unittest

from graphql import (
    GraphQLSchema,
    validate_schema,
)

from minos.common import (
    MinosConfig,
)
from minos.plugins.graphql import (
    GraphQlEnroute,
    GraphQlHttpRouter,
)
from tests.utils import (
    BASE_PATH,
)


class TestSomething(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = MinosConfig(CONFIG_FILE_PATH)

    def test_from_config(self):
        router = GraphQlHttpRouter.from_config(config=self._config)

        self.assertIsInstance(router._schema, GraphQLSchema)
        self.assertIsInstance(router, GraphQlHttpRouter)


if __name__ == "__main__":
    unittest.main()
