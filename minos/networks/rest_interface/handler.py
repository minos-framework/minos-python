# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

from aiohttp import web
from minos.common.configuration.config import (
    MinosConfig,
)
from minos.common.importlib import (
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
        for item in self._config.rest.endpoints:
            callable_f = self.class_resolver(item.controller, item.action)
            self._app.router.add_route(item.method, item.route, callable_f)

    @staticmethod
    def class_resolver(controller: str, action: str):
        object_class = import_module(controller)
        instance_class = object_class()
        class_method = getattr(instance_class, action)

        return class_method

    def get_app(self):
        return self._app
