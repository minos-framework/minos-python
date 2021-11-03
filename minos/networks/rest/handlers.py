from __future__ import (
    annotations,
)

import logging
from functools import (
    cached_property,
)
from inspect import (
    isawaitable,
)
from typing import (
    Awaitable,
    Callable,
    Optional,
    Union,
)

from aiohttp import (
    web,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)

from ..decorators import (
    EnrouteBuilder,
)
from ..messages import (
    USER_CONTEXT_VAR,
    Response,
    ResponseException,
)
from .messages import (
    RestRequest,
    RestResponse,
)

logger = logging.getLogger(__name__)


class RestHandler(MinosSetup):
    """Rest Handler class."""

    def __init__(self, host: str, port: int, endpoints: dict[(str, str), Callable], **kwargs):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._endpoints = endpoints

    @property
    def endpoints(self) -> dict[(str, str), Callable]:
        """Endpoints getter.

        :return: A dictionary value.
        """
        return self._endpoints

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> RestHandler:
        host = config.rest.host
        port = config.rest.port
        endpoints = cls._endpoints_from_config(config)

        return cls(host=host, port=port, endpoints=endpoints, **kwargs)

    @staticmethod
    def _endpoints_from_config(config: MinosConfig, **kwargs) -> dict[(str, str), Callable]:
        builder = EnrouteBuilder(config.commands.service, config.queries.service)
        decorators = builder.get_rest_command_query(config=config, **kwargs)
        endpoints = {(decorator.url, decorator.method): fn for decorator, fn in decorators.items()}
        return endpoints

    @property
    def host(self) -> str:
        """Get the rest host.

        :return: A ``str`` object.
        """
        return self._host

    @property
    def port(self) -> int:
        """Get the rest port.

        :return: An integer value.
        """
        return self._port

    def get_app(self) -> web.Application:
        """Return rest application instance.

        :return: A `web.Application` instance.
        """
        return self._app

    @cached_property
    def _app(self) -> web.Application:
        app = web.Application()
        self._mount_routes(app)
        return app

    def _mount_routes(self, app: web.Application):
        """Load routes from config file."""
        for (url, method), fn in self._endpoints.items():
            self._mount_one_route(method, url, fn, app)

        # Load default routes
        self._mount_system_health(app)

    def _mount_one_route(self, method: str, url: str, action: Callable, app: web.Application) -> None:
        handler = self.get_callback(action)
        app.router.add_route(method, url, handler)

    @staticmethod
    def get_callback(
        fn: Callable[[RestRequest], Union[Optional[RestResponse], Awaitable[Optional[RestResponse]]]]
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Get the handler function to be used by the ``aiohttp`` Controller.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the ``aiohttp`` Controller.
        """

        async def _fn(request: web.Request) -> web.Response:
            logger.info(f"Dispatching '{request!s}' from '{request.remote!s}'...")

            request = RestRequest(request)
            token = USER_CONTEXT_VAR.set(request.user)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response
                if isinstance(response, Response):
                    response = await response.raw_content()
                if response is None:
                    return web.json_response()
                return web.json_response(response)
            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
                raise web.HTTPBadRequest(text=str(exc))
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")
                raise web.HTTPInternalServerError()
            finally:
                USER_CONTEXT_VAR.reset(token)

        return _fn

    def _mount_system_health(self, app: web.Application):
        """Mount System Health Route."""
        app.router.add_get("/system/health", self._system_health_handler)

    @staticmethod
    async def _system_health_handler(request: web.Request) -> web.Response:
        """System Health Route Handler.
        :return: A `web.json_response` response.
        """
        logger.info(f"Dispatching '{request!s}' from '{request.remote!s}'...")
        return web.json_response({"host": request.host})
