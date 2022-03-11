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
from typing import (
    Callable,
    Optional,
    Union,
)

from aiohttp import (
    web,
)
from aiohttp.web_runner import (
    AppRunner,
    SockSite,
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
    """TODO"""

    def __init__(self, *args, shutdown_timeout: int = 6, **kwargs):
        super().__init__(*args, **kwargs)
        self.socket = bind_socket(address=self._host, port=self._port, proto_name="http")

        self.shutdown_timeout = shutdown_timeout
        self.runner = None
        self.site = None

    @cached_property
    def application(self) -> web.Application:
        """TODO

        :return: TODO
        """
        return web.Application()

    def _mount_route(self, path: str, method: str, adapted_callback: Callable):
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
        self.runner = AppRunner(self.application)
        await self.runner.setup()

        self.site = SockSite(self.runner, self.socket, shutdown_timeout=self.shutdown_timeout)
        await self.site.start()

    async def _stop(self, exception: Exception = None) -> None:
        try:
            if self.site:
                await self.site.stop()
        finally:
            if hasattr(self, "runner"):
                await self.runner.cleanup()
