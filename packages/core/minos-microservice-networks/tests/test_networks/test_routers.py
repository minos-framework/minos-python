import unittest
from abc import (
    ABC,
)
from typing import (
    Callable,
)

from minos.common import (
    MinosConfig,
)
from minos.networks import (
    BrokerRouter,
    EnrouteBuilder,
    EnrouteDecorator,
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

    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        return routes


class TestRouter(unittest.TestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(CONFIG_FILE_PATH)

    def test_is_subclass(self):
        self.assertTrue(issubclass(Router, ABC))

    def test_constructor(self):
        builder = EnrouteBuilder(*self.config.services, middleware=self.config.middleware)
        router = _Router.from_config(self.config)
        self.assertEqual(builder.get_all().keys(), router.routes.keys())

    def test_eq(self):
        def _fn():
            pass

        router = _Router.from_config(self.config)
        router_eq = _Router.from_config(self.config)
        router_ne1 = _Router.from_config(self.config)
        router_ne1.routes.clear()
        router_ne2 = _Router.from_config(self.config)
        router_ne2.routes[next(iter(router_ne2.routes.keys()))] = _fn
        router_ne3 = 1234

        self.assertEqual(router_eq, router)
        self.assertNotEqual(router_ne1, router)
        self.assertNotEqual(router_ne2, router)
        self.assertNotEqual(router_ne3, router)


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
