import logging
import traceback
from collections.abc import (
    Awaitable,
)
from functools import (
    wraps,
)
from inspect import (
    isawaitable,
)
from socket import (
    socket,
)
from typing import (
    Callable,
    Optional,
    Union,
)

from aiohttp import (
    web,
    web_runner,
)
from aiomisc import (
    bind_socket,
)
from cached_property import (
    cached_property,
)

from minos.networks import (
    REQUEST_USER_CONTEXT_VAR,
    HttpConnector,
    Request,
    Response,
    ResponseException,
)

from .requests import (
    AioHttpRequest,
    AioHttpResponse,
)

logger = logging.getLogger(__name__)


class AioHttpConnector(HttpConnector):
    """AioHttp Connector class."""

    def __init__(self, *args, shutdown_timeout: float = 6, **kwargs):
        super().__init__(*args, **kwargs)

        self._shutdown_timeout = shutdown_timeout
        self._socket = None
        self._runner = None
        self._site = None

    @property
    def shutdown_timeout(self) -> float:
        """Get the shutdown timeout.

        :return: A ``float`` value.
        """
        return self._shutdown_timeout

    @property
    def runner(self) -> Optional[web_runner.AppRunner]:
        """Get the application runner.

        :return: An ``AppRunner`` instance or ``None``.
        """
        return self._runner

    @property
    def socket(self) -> Optional[socket]:
        """Get the socket.

        :return: A ``socket`` value or ``None``.
        """
        return self._socket

    @property
    def site(self) -> Optional[web_runner.SockSite]:
        """Get the application site.

        :return: A ``SockSite`` instance or ``None``.
        """
        return self._site

    @cached_property
    def application(self) -> web.Application:
        """Get the application.

        :return: An ``Application`` instance.
        """
        return web.Application()

    def _mount_route(self, path: str, method: str, adapted_callback: Callable) -> None:
        self.application.router.add_route(method, path, adapted_callback)

    def _adapt_callback(
        self, callback: Callable[[Request], Union[Optional[Response], Awaitable[Optional[Response]]]]
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Get the handler function to be used by the ``aiohttp`` Controller.

        :param callback: The action function.
        :return: A wrapper function around the given one that is compatible with the ``aiohttp`` Controller.
        """

        @wraps(callback)
        async def _wrapper(request: web.Request) -> web.Response:
            logger.info(f"Dispatching '{request!s}' from '{request.remote!s}'...")

            request = AioHttpRequest(request)
            token = REQUEST_USER_CONTEXT_VAR.set(request.user)

            try:
                response = callback(request)
                if isawaitable(response):
                    response = await response

                if not isinstance(response, AioHttpResponse):
                    response = AioHttpResponse.from_response(response)

                content = await response.content()
                content_type = response.content_type
                status = response.status

                return web.Response(body=content, content_type=content_type, status=status)

            except ResponseException as exc:
                tb = traceback.format_exc()
                logger.error(f"Raised an application exception:\n {tb}")
                return web.Response(text=tb, status=exc.status)
            except Exception:
                tb = traceback.format_exc()
                logger.exception(f"Raised a system exception:\n {tb}")
                raise web.HTTPInternalServerError()
            finally:
                REQUEST_USER_CONTEXT_VAR.reset(token)

        return _wrapper

    async def _start(self) -> None:
        self._runner = web_runner.AppRunner(self.application)
        await self._runner.setup()

        self._socket = bind_socket(address=self._host, port=self._port, proto_name="http")
        self._site = web_runner.SockSite(self._runner, self._socket, shutdown_timeout=self._shutdown_timeout)
        await self._site.start()

    async def _stop(self, exception: Exception = None) -> None:
        if self._site is not None:
            await self._site.stop()
            self._site = None

        if self._socket is not None:
            self._socket.close()
            self._socket = None

        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
