from __future__ import (
    annotations,
)

from abc import (
    ABC,
)
from typing import (
    Any,
    Callable,
    Optional,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from . import (
    HttpEnrouteDecorator,
)
from .decorators import (
    EnrouteBuilder,
    EnrouteDecorator,
)


class Router(MinosSetup, ABC):
    """Router base class."""

    def __init__(self, routes: Optional[dict[EnrouteDecorator, Callable]] = None, **kwargs):
        super().__init__(**kwargs)

        if routes is None:
            routes = dict()

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

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False

        if self._routes.keys() != other._routes.keys():
            return False

        for key in self._routes:
            if self._routes[key].__qualname__ != other._routes[key].__qualname__:
                return False

        return True


class HttpRouter(Router, ABC):
    """Http Router base class."""

    routes: dict[HttpEnrouteDecorator, Callable]


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
