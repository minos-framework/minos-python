from __future__ import (
    annotations,
)

import logging
import traceback
from abc import (
    ABC,
    abstractmethod,
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
    MinosConfig,
    MinosSetup,
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


class HttpConnector(ABC, MinosSetup, Generic[RawRequest, RawResponse]):
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

    async def start(self) -> None:
        """Start the connector.

        :return: This method does not return anything.
        """

        self.mount_routes()
        await self._start()

    async def stop(self) -> None:
        """Stop the connector.

        :return: This method does not return anything.
        """
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
