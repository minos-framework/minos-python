from __future__ import (
    annotations,
)

import logging
from collections.abc import (
    Callable,
)
from typing import (
    Any,
)

from minos.common import (
    Config,
    SetupMixin,
    import_module,
)

from ..decorators import (
    HttpEnrouteDecorator,
)
from ..exceptions import (
    MinosRedefinedEnrouteDecoratorException,
)
from ..routers import (
    HttpRouter,
)

logger = logging.getLogger(__name__)


class HttpAdapter(SetupMixin):
    """Http Adapter class."""

    def __init__(self, routers: list[HttpRouter], **kwargs):
        super().__init__(**kwargs)
        self._routers = routers

    @classmethod
    def _from_config(cls, *args, config: Config, **kwargs) -> HttpAdapter:
        routers = cls._routers_from_config(config)
        return cls(routers=routers, **kwargs)

    @staticmethod
    def _routers_from_config(config: Config, **kwargs) -> list[HttpRouter]:
        classes = config.get_routers()
        classes = tuple((class_ if not isinstance(class_, str) else import_module(class_)) for class_ in classes)
        classes = filter(lambda router: issubclass(router, HttpRouter), classes)
        routers = [router.from_config(config) for router in classes]
        return routers

    @property
    def routes(self) -> dict[HttpEnrouteDecorator, Callable]:
        """Get routes.

        :return: A ``dict`` with ``HttpEnrouteDecorator`` and ``Callable`` as values.
        """
        routes = dict()
        for router in self._routers:
            for decorator, callback in router.routes.items():
                if decorator in routes:
                    raise MinosRedefinedEnrouteDecoratorException(f"{decorator!r} can be used only once.")
                routes[decorator] = callback
        return routes

    @property
    def routers(self) -> list[HttpRouter]:
        """Get the routers.

        :return: A ``list`` of ``Router`` instances.
        """
        return self._routers

    def __eq__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and self._routers == other._routers
