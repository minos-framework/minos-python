import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerRouter,
    EnrouteBuilder,
    HttpRouter,
    PeriodicRouter,
    RestHttpRouter,
    Router,
)
from tests.utils import (
    CONFIG_FILE_PATH,
)


class _Router(Router):
    """For testing purposes."""


class TestRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(Router, ABC))

    def test_routes(self):
        builder = EnrouteBuilder(*self.config.services, middleware=self.config.middleware)
        router = _Router.from_config(self.config)
        self.assertEqual(builder.get_all().keys(), router.routes.keys())


class TestHttpRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(HttpRouter, (ABC, Router)))


class TestRestHttpRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(RestHttpRouter, Router))

    def test_routes(self):
        builder = EnrouteBuilder(*self.config.services, middleware=self.config.middleware)
        router = RestHttpRouter.from_config(self.config)
        self.assertEqual(builder.get_rest_command_query().keys(), router.routes.keys())


class TestBrokerRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(BrokerRouter, Router))

    def test_routes(self):
        builder = EnrouteBuilder(*self.config.services, middleware=self.config.middleware)
        router = BrokerRouter.from_config(self.config)
        self.assertEqual(builder.get_broker_command_query_event().keys(), router.routes.keys())


class TestPeriodicRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(PeriodicRouter, Router))

    def test_routes(self):
        builder = EnrouteBuilder(*self.config.services, middleware=self.config.middleware)
        router = PeriodicRouter.from_config(self.config)
        self.assertEqual(builder.get_periodic_event().keys(), router.routes.keys())


if __name__ == "__main__":
    unittest.main()
