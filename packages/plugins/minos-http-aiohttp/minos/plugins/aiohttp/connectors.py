import logging
from collections.abc import (
    Callable,
)
from typing import (
    Optional,
)

from aiohttp import (
    web,
    web_runner,
)
from cached_property import (
    cached_property,
)

from minos.networks import (
    HttpConnector,
    Request,
    Response,
)

from .requests import (
    AioHttpRequest,
    AioHttpResponse,
)

logger = logging.getLogger(__name__)


class AioHttpConnector(HttpConnector[web.Request, web.Response]):
    """AioHttp Connector class."""

    def __init__(self, *args, shutdown_timeout: float = 6, **kwargs):
        super().__init__(*args, **kwargs)

        self._shutdown_timeout = shutdown_timeout
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
    def site(self) -> Optional[web_runner.TCPSite]:
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

    async def _build_request(self, request: web.Request) -> Request:
        return AioHttpRequest(request)

    async def _build_response(self, response: Optional[Response]) -> web.Response:
        if not isinstance(response, AioHttpResponse):
            response = AioHttpResponse.from_response(response)

        content = await response.content()
        content_type = response.content_type
        status = response.status

        return web.Response(body=content, content_type=content_type, status=status)

    async def _build_error_response(self, message: str, status: int) -> web.Response:
        return web.Response(text=message, status=status)

    async def _start(self) -> None:
        self._runner = web_runner.AppRunner(self.application, access_log=None)
        await self._runner.setup()

        self._site = web_runner.TCPSite(
            runner=self._runner,
            host=self._host,
            port=self._port,
            reuse_address=True,
            reuse_port=False,
            shutdown_timeout=self._shutdown_timeout,
        )
        await self._site.start()

    async def _stop(self, exception: Exception = None) -> None:
        if self._site is not None:
            await self._site.stop()
            self._site = None

        if self._runner is not None:
            await self._runner.cleanup()
            self._runner = None
