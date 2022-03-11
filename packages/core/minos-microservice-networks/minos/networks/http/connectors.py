from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from collections.abc import (
    Callable,
)
from typing import (
    Awaitable,
    Optional,
    Union,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from ..decorators import (
    HttpEnrouteDecorator,
)
from ..requests import (
    Request,
    Response,
)
from .adapters import (
    HttpAdapter,
)

_Callback = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]


class HttpConnector(ABC, MinosSetup):
    """Http Application base class."""

    def __init__(self, host: str, port: int, adapter: HttpAdapter, **kwargs):
        super().__init__(**kwargs)

        self._host = host
        self._port = port
        self._adapter = adapter

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> HttpConnector:
        host = config.rest.host
        port = config.rest.port
        adapter = HttpAdapter.from_config(config)
        return cls(host, port, adapter)

    async def _setup(self) -> None:
        await super()._setup()
        self._mount_routes()
        await self._start()

    def _mount_routes(self):
        for decorator, callback in self.routes.items():
            self.mount_route(decorator.path, decorator.method, callback)

    async def _destroy(self) -> None:
        await self._stop()
        await super()._destroy()

    @abstractmethod
    async def _start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _stop(self) -> None:
        raise NotImplementedError

    def mount_route(self, path: str, method: str, callback: Callable[[Request], Optional[Response]]):
        """Mount a new route on the application.

        :param path: The request's path.
        :param method: The request's method.
        :param callback: The callback to be executed.
        :return: This method does not return anything.
        """
        adapted_callback = self._adapt_callback(callback)
        self._mount_route(path, method, adapted_callback)

    @abstractmethod
    def _mount_route(self, path: str, method: str, adapted_callback: Callable):
        raise NotImplementedError

    @abstractmethod
    def _adapt_callback(self, callback: _Callback) -> Callable:
        raise NotImplementedError

    @property
    def host(self) -> str:
        """Get the host.

        :return: A ``str`` value.
        """
        return self._host

    @property
    def port(self) -> int:
        """Get the port.

        :return: A ``int`` value.
        """
        return self._port

    @property
    def routes(self) -> dict[HttpEnrouteDecorator, _Callback]:
        """Get the port.

        :return: A ``int`` value.
        """
        return self.adapter.routes

    @property
    def adapter(self) -> HttpAdapter:
        """Get the port.

        :return: A ``int`` value.
        """
        return self._adapter
