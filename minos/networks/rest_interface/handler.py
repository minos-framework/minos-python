# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from aiohttp import (
    web,
)

from minos.common import (
    MinosConfig,
    import_module,
)


class RestInterfaceHandler:
    """
    Rest Interface Handler

    Rest Interface for aiohttp web handling.

    """

    __slots__ = "_config", "_app"

    def __init__(self, config: MinosConfig, app: web.Application = web.Application()):
        self._config = config
        self._app = app
        self.load_routes()

    def load_routes(self):
        """Load routes from config file."""
        for item in self._config.rest.endpoints:
            callable_f = self.class_resolver(item.controller, item.action)
            self._app.router.add_route(item.method, item.route, callable_f)

        # Load default routes
        self._mount_system_health()

    @staticmethod
    def class_resolver(controller: str, action: str):
        """Load controller class and action method.
        :param controller: Controller string. Example: "tests.service.CommandTestService.CommandService"
        :param action: Config instance. Example: "get_order"
        :return: A class method callable instance.
        """
        object_class = import_module(controller)
        instance_class = object_class()
        class_method = getattr(instance_class, action)

        return class_method

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
