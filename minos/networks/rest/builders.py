# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

import logging
from functools import (
    cached_property,
)
from inspect import (
    isawaitable,
    isclass,
)
from typing import (
    Awaitable,
    Callable,
    NoReturn,
    Optional,
    Union,
)

from aiohttp import (
    web,
)

from minos.common import (
    ENDPOINT,
    MinosConfig,
    MinosException,
    MinosSetup,
    Response,
    ResponseException,
    classname,
    import_module,
)

from .messages import (
    HttpRequest,
    HttpResponse,
)

logger = logging.getLogger(__name__)


class RestBuilder(MinosSetup):
    """
    Rest Interface Handler

    Rest Interface for aiohttp web handling.

    """

    def __init__(self, host: str, port: int, endpoints: list[ENDPOINT], **kwargs):
        super().__init__(**kwargs)
        self._host = host
        self._port = port
        self._endpoints = endpoints

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> RestBuilder:
        host = config.rest.broker.host
        port = config.rest.broker.port
        return cls(host=host, port=port, endpoints=config.rest.endpoints, **kwargs)

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
        for item in self._endpoints:
            self._mount_one_route(item, app)

        # Load default routes
        self._mount_system_health(app)

    def _mount_one_route(self, item: ENDPOINT, app: web.Application) -> NoReturn:
        action = self.get_action(item.controller, item.action)
        handler = self.get_callback(action)
        app.router.add_route(item.method, item.route, handler)

    @staticmethod
    def get_action(
        controller: str, action: str
    ) -> Callable[[HttpRequest], Union[Optional[HttpResponse], Awaitable[Optional[HttpResponse]]]]:
        """Load controller class and action method.
        :param controller: Controller string. Example: "tests.service.CommandTestService.CommandService"
        :param action: Config instance. Example: "get_order"
        :return: A class method callable instance.
        """
        controller = import_module(controller)
        if isclass(controller):
            controller = controller()
        action_fn = getattr(controller, action)
        return action_fn

    @staticmethod
    def get_callback(
        fn: Callable[[HttpRequest], Union[Optional[HttpResponse], Awaitable[Optional[HttpResponse]]]]
    ) -> Callable[[web.Request], Awaitable[web.Response]]:
        """Get the handler function to be used by the ``aiohttp`` Controller.

        :param fn: The action function.
        :return: A wrapper function around the given one that is compatible with the ``aiohttp`` Controller.
        """

        async def _fn(request: web.Request) -> web.Response:
            logger.info(f"Dispatching {classname(fn)!r} from {request.remote!r}...")
            request = HttpRequest(request)

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
                logger.info(f"Raised a user exception: {exc!s}")
                raise web.HTTPBadRequest(text=str(exc))
            except MinosException as exc:
                logger.warning(f"Raised a 'minos' exception: {exc!r}")
                raise web.HTTPInternalServerError()
            except Exception as exc:
                logger.exception(f"Raised an exception: {exc!r}.")
                raise web.HTTPInternalServerError()

        return _fn

    def _mount_system_health(self, app: web.Application):
        """Mount System Health Route."""
        app.router.add_get("/system/health", self._system_health_handler)

    @staticmethod
    async def _system_health_handler(request: web.Request) -> web.Response:
        """System Health Route Handler.
        :return: A `web.json_response` response.
        """
        logger.info(f"Dispatching 'health' from {request.remote!r}...")
        return web.json_response({"host": request.host}, status=200)
