import unittest

from minos.common import MinosConfig

from minos.plugins.graphql import (
    GraphQlEnroute,
    GraphQlHttpRouter,
)
from tests.utils import (
    BASE_PATH,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)


class TestSomething(unittest.TestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"
    _config = MinosConfig(CONFIG_FILE_PATH)

    def test_true(self):
        router = GraphQlHttpRouter.from_config(config=self._config)
        self.assertTrue(GraphQlEnroute)


if __name__ == '__main__':
    unittest.main()
