from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from typing import (
    Callable,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from .decorators import (
    EnrouteBuilder,
    EnrouteDecorator,
)


class Router(MinosSetup, ABC):
    """Router base class."""

    def __init__(self, routes: dict[EnrouteDecorator, Callable], **kwargs):
        super().__init__(**kwargs)
        self._routes = routes

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> Router:
        routes = cls._routes_from_config(config)
        return cls(routes)

    @staticmethod
    def _routes_from_config(config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_all(config=config, **kwargs)
        return routes

    @property
    def routes(self) -> dict[EnrouteDecorator, Callable]:
        """Get the routes stored on the router.

        :return: A dict with decorators as keys and callbacks as values.
        """
        return self._routes


class HttpRouter(Router, ABC):
    """Http Router base class."""


class RestHttpRouter(HttpRouter):
    """Rest Http Router class."""

    @classmethod
    def _routes_from_config(cls, config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_rest_command_query(config=config, **kwargs)
        return routes


class BrokerRouter(Router):
    """Broker Router class."""

    @staticmethod
    def _routes_from_config(config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_broker_command_query_event(config=config, **kwargs)
        return routes


class PeriodicRouter(Router):
    """Periodic Router class."""

    @staticmethod
    def _routes_from_config(config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
        routes = builder.get_periodic_event(config=config, **kwargs)
        return routes
