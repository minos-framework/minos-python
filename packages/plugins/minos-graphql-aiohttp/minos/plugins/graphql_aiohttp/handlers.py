from __future__ import (
    annotations,
)

import logging
from functools import (
    cached_property,
    wraps,
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

import aiohttp_jinja2
import jinja2
from aiohttp import (
    web,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
)
from minos.networks import (
    REQUEST_USER_CONTEXT_VAR,
    EnrouteBuilder,
    ResponseException,
)

from .handlers import GraphiqlHandler, GraphqlHandler
from .requests import (
    RestRequest,
)
from .responses import (
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
        builder = EnrouteBuilder(*config.services, middleware=config.middleware)
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
        await self.initialize_jinja2(app)
        self.register_handlers(app)
        # self._mount_routes(app)
        # self._mount_graphql(app)
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

    """
    def _mount_graphql(self, app: web.Application):
        GraphQLView.attach(app, schema=schema, graphiql=True)
    """

    def register_handlers(self, app: web.Application):
        routes = [(r"/graphql", GraphqlHandler)]

        # if self.dev:
        routes += [(r"/graphiql", GraphiqlHandler)]

        app.router.add_routes([web.view(route[0], route[1]) for route in routes])

    async def initialize_jinja2(self, app: web.Application):
        aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader("./templates"))

    @staticmethod
    def get_callback(
        fn: Callable[[RestRequest], Union[Optional[RestResponse], Awaitable[Optional[RestResponse]]]]
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Get the handler function to be used by the ``aiohttp`` Controller.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the ``aiohttp`` Controller.
        """

        @wraps(fn)
        async def _wrapper(request: web.Request) -> web.Response:
            logger.info(f"Dispatching '{request!s}' from '{request.remote!s}'...")

            request = RestRequest(request)
            token = REQUEST_USER_CONTEXT_VAR.set(request.user)

            try:
                response = fn(request)
                if isawaitable(response):
                    response = await response

                if not isinstance(response, RestResponse):
                    response = RestResponse.from_response(response)

                content = await response.content()
                content_type = response.content_type
                status = response.status

                return web.Response(body=content, content_type=content_type, status=status)

            except ResponseException as exc:
                logger.warning(f"Raised an application exception: {exc!s}")
                return web.Response(text=str(exc), status=exc.status)
            except Exception as exc:
                logger.exception(f"Raised a system exception: {exc!r}")
                raise web.HTTPInternalServerError()
            finally:
                REQUEST_USER_CONTEXT_VAR.reset(token)

        return _wrapper

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
