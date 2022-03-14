import unittest

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    HttpAdapter,
    HttpRouter,
    MinosRedefinedEnrouteDecoratorException,
)
from tests.test_networks.test_routers import (
    _Router,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class TestHttpAdapter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)
        self.adapter = HttpAdapter.from_config(self.config)

    def test_routers(self):
        self.assertEqual(1, len(self.adapter.routers))
        for router in self.adapter.routers:
            self.assertIsInstance(router, HttpRouter)

    def test_routes(self):
        expected = dict()
        for router in self.adapter.routers:
            expected |= router.routes

        observed = self.adapter.routes
        self.assertEqual(expected, observed)

    def test_routes_raises(self):
        router = self.adapter.routers[0]

        adapter = HttpAdapter([router, router])
        with self.assertRaises(MinosRedefinedEnrouteDecoratorException):
            adapter.routes

    def test_eq(self):
        another_eq = HttpAdapter.from_config(self.config)
        another_ne = HttpAdapter([_Router.from_config(self.config)])

        self.assertEqual(another_eq, self.adapter)
        self.assertNotEqual(another_ne, self.adapter)


if __name__ == "__main__":
    unittest.main()
