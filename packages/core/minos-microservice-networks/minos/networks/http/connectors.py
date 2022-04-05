from __future__ import (
    annotations,
)

import logging
import traceback
from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    Semaphore,
)
from collections.abc import (
    Callable,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from typing import (
    Awaitable,
    Generic,
    Optional,
    TypeVar,
    Union,
)

from minos.common import (
    Config,
    Injectable,
    SetupMixin,
)

from ..decorators import (
    HttpEnrouteDecorator,
)
from ..requests import (
    REQUEST_USER_CONTEXT_VAR,
    Request,
    Response,
    ResponseException,
)
from .adapters import (
    HttpAdapter,
)

_Callback = Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]

RawRequest = TypeVar("RawRequest")
RawResponse = TypeVar("RawResponse")

logger = logging.getLogger(__name__)


@Injectable("http_connector")
class HttpConnector(ABC, SetupMixin, Generic[RawRequest, RawResponse]):
    """Http Application base class."""

    def __init__(
        self,
        adapter: HttpAdapter,
        host: Optional[str] = None,
        port: Optional[int] = None,
        max_connections: int = 5,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if host is None:
            host = "0.0.0.0"
        if port is None:
            port = 8080

        self._adapter = adapter
        self._host = host
        self._port = port

        self._semaphore = Semaphore(max_connections)

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> HttpConnector:
        http_config = config.get_interface_by_name("http")
        connector_config = http_config["connector"]

        host = connector_config.get("host")
        port = connector_config.get("port")
        adapter = HttpAdapter.from_config(config)

        return cls(adapter, host, port)

    async def start(self) -> None:
        """Start the connector.

        :return: This method does not return anything.
        """

        logger.info(f"Starting {self!r}...")
        self.mount_routes()
        await self._start()

    async def stop(self) -> None:
        """Stop the connector.

        :return: This method does not return anything.
        """
        logger.info(f"Stopping {self!r}...")
        await self._stop()

    @abstractmethod
    async def _start(self) -> None:
        raise NotImplementedError

    @abstractmethod
    async def _stop(self) -> None:
        raise NotImplementedError

    def mount_routes(self) -> None:
        """Mount the routes given by the adapter.

        :return: This method does not return anything.
        """
        for decorator, callback in self.routes.items():
            self.mount_route(decorator.path, decorator.method, callback)

    def mount_route(self, path: str, method: str, callback: Callable[[Request], Optional[Response]]):
        """Mount a new route on the application.

        :param path: The request's path.
        :param method: The request's method.
        :param callback: The callback to be executed.
        :return: This method does not return anything.
        """
        adapted_callback = self.adapt_callback(callback)
        self._mount_route(path, method, adapted_callback)

    @abstractmethod
    def _mount_route(self, path: str, method: str, adapted_callback: Callable) -> None:
        raise NotImplementedError

    def adapt_callback(
        self, callback: Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]
    ) -> Callable[[RawRequest], Awaitable[RawResponse]]:
        """Get the adapted callback to be used by the connector.

        :param callback: The function.
        :return: A wrapper function on top of the given one that is compatible with the connector.
        """

        @wraps(callback)
        async def _wrapper(raw: RawRequest) -> RawResponse:
            async with self._semaphore:
                logger.info(f"Dispatching '{raw!s}'...")

                request = await self._build_request(raw)
                token = REQUEST_USER_CONTEXT_VAR.set(request.user)

                # noinspection PyBroadException
                try:
                    response = callback(request)
                    if isawaitable(response):
                        response = await response

                    return await self._build_response(response)

                except ResponseException as exc:
                    tb = traceback.format_exc()
                    logger.error(f"Raised an application exception:\n {tb}")
                    return await self._build_error_response(tb, exc.status)
                except Exception:
                    tb = traceback.format_exc()
                    logger.exception(f"Raised a system exception:\n {tb}")
                    return await self._build_error_response(tb, 500)
                finally:
                    REQUEST_USER_CONTEXT_VAR.reset(token)

        return _wrapper

    @abstractmethod
    async def _build_request(self, request: RawRequest) -> Request:
        raise NotImplementedError

    @abstractmethod
    async def _build_response(self, response: Optional[Response]) -> RawResponse:
        raise NotImplementedError

    @abstractmethod
    async def _build_error_response(self, message: str, status: int) -> RawResponse:
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

    def __repr__(self):
        return f"{type(self).__name__}({self._host!r}, {self._port})"
