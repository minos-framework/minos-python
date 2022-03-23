from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
    Callable,
)

from cached_property import (
    cached_property,
)

from minos.common import (
    Config,
    SetupMixin,
)

from .decorators import (
    BrokerEnrouteDecorator,
    EnrouteDecorator,
    EnrouteFactory,
    HttpEnrouteDecorator,
    PeriodicEnrouteDecorator,
)


class Router(ABC, SetupMixin):
    """Router base class."""

    def __init__(self, config: Config, **kwargs):
        super().__init__(**kwargs)

        self._config = config

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> Router:
        return cls(config)

    @cached_property
    def routes(self) -> dict[EnrouteDecorator, Callable]:
        """Get the routes stored on the router.

        :return: A dict with decorators as keys and callbacks as values.
        """
        return self._build_routes()

    def _build_routes(self) -> dict[EnrouteDecorator, Callable]:
        routes = self._get_all_routes()
        routes = self._filter_routes(routes)
        return routes

    def _get_all_routes(self) -> dict[EnrouteDecorator, Callable]:
        builder = EnrouteFactory(*self._config.get_services(), middleware=self._config.get_middleware())
        routes = builder.get_all(config=self._config)
        return routes

    @abstractmethod
    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        raise NotImplementedError

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, type(self)):
            return False

        if self.routes.keys() != other.routes.keys():
            return False

        for key in self.routes:
            if self.routes[key].__qualname__ != other.routes[key].__qualname__:
                return False

        return True


class HttpRouter(Router, ABC):
    """Http Router base class."""

    routes: dict[HttpEnrouteDecorator, Callable]


class RestHttpRouter(HttpRouter):
    """Rest Http Router class."""

    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        routes = {
            decorator: callback for decorator, callback in routes.items() if isinstance(decorator, HttpEnrouteDecorator)
        }
        return routes


class BrokerRouter(Router):
    """Broker Router class."""

    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        routes = {
            decorator: callback
            for decorator, callback in routes.items()
            if isinstance(decorator, BrokerEnrouteDecorator)
        }
        return routes


class PeriodicRouter(Router):
    """Periodic Router class."""

    def _filter_routes(self, routes: dict[EnrouteDecorator, Callable]) -> dict[EnrouteDecorator, Callable]:
        routes = {
            decorator: callback
            for decorator, callback in routes.items()
            if isinstance(decorator, PeriodicEnrouteDecorator)
        }
        return routes
