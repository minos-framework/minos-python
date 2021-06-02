# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.
from __future__ import (
    annotations,
)

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
    import_module,
)

from .messages import (
    HttpRequest,
)


class RestBuilder(MinosSetup):
    """
    Rest Interface Handler

    Rest Interface for aiohttp web handling.

    """

    __slots__ = "_config", "_app"

    def __init__(self, config: MinosConfig, app: web.Application = web.Application(), **kwargs):
        super().__init__(**kwargs)
        self._config = config
        self._app = app
        self.load_routes()

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> RestBuilder:
        return cls(*args, config=config, **kwargs)

    def load_routes(self):
        """Load routes from config file."""
        for item in self._config.rest.endpoints:
            callable_f = self.resolve_action(item.controller, item.action)
            self._app.router.add_route(item.method, item.route, callable_f)

        # Load default routes
        self._mount_system_health()

    @staticmethod
    def resolve_action(controller: str, action: str) -> Callable:
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

    def get_app(self):
        """Return rest application instance.
        :return: A `web.Application` instance.
        """
        return self._app

    def _mount_system_health(self):
        """Mount System Health Route."""
        self._app.router.add_get("/system/health", self._system_health_handler)

    @staticmethod
    async def _system_health_handler(request):
        """System Health Route Handler.
        :return: A `web.json_response` response.
        """
        return web.json_response({"host": request.host}, status=200)
