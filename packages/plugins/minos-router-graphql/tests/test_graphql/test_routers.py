import unittest

from minos.common import (
    Config,
)
from minos.networks import (
    HttpEnrouteDecorator,
)
from minos.plugins.graphql import (
    GraphQlHttpRouter,
)
from tests.utils import (
    BASE_PATH,
)


class TestGraphQlHttpRouter(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = Config(CONFIG_FILE_PATH)

    def test_from_config(self):
        router = GraphQlHttpRouter.from_config(self._config)

        self.assertIsInstance(router, GraphQlHttpRouter)
        self.assertEqual(
            {HttpEnrouteDecorator("/foo/graphql", "POST"), HttpEnrouteDecorator("/foo/graphql/schema", "GET")},
            router.routes.keys(),
        )


if __name__ == "__main__":
    unittest.main()
