# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

from functools import cached_property
from inspect import (
    isawaitable,
)
from typing import (
    Callable,
)

from aiohttp import (
    web,
)

from minos.common import (
    MinosConfig,
    MinosSetup,
    import_module, ENDPOINT,
)

from .messages import (
    HttpRequest,
)


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

    @property
    def host(self) -> str:
        """TODO

        :return: TODO
        """
        return self._host

    @property
    def port(self) -> int:
        """TODO

        :return: TODO
        """
        return self._port

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> RestBuilder:
        host = config.rest.broker.host
        port = config.rest.broker.port
        return cls(host=host, port=port, endpoints=config.rest.endpoints, **kwargs)

    def get_app(self):
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
            callable_f = self.get_action(item.controller, item.action)
            app.router.add_route(item.method, item.route, callable_f)

        # Load default routes
        self._mount_system_health(app)

    @staticmethod
    def get_action(controller: str, action: str) -> Callable:
        """Load controller class and action method.
        :param controller: Controller string. Example: "tests.service.CommandTestService.CommandService"
        :param action: Config instance. Example: "get_order"
        :return: A class method callable instance.
        """
        object_class = import_module(controller)
        instance_class = object_class()
        class_method = getattr(instance_class, action)

        async def _fn(request: web.Request) -> web.Response:
            request = HttpRequest(request)
            response = class_method(request)
            if isawaitable(response):
                response = await response
            return web.json_response(await response.raw_content())

        return _fn

    def _mount_system_health(self, app: web.Application):
        """Mount System Health Route."""
        app.router.add_get("/system/health", self._system_health_handler)

    @staticmethod
    async def _system_health_handler(request):
        """System Health Route Handler.
        :return: A `web.json_response` response.
        """
        return web.json_response({"host": request.host}, status=200)
