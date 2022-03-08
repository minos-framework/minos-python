import aiohttp_jinja2
import jinja2
from aiohttp import (
    web,
)
from aiomisc.service.aiohttp import (
    AIOHTTPService,
)

from .graphql_example import (
    schema,
)
from .handlers.graphiql import GraphiqlHandler
from .handlers.graphql import GraphqlHandler


class RestService(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, **kwargs):
        # self.handler = RestHandler.from_config(**kwargs)
        # super().__init__(**(kwargs | {"address": self.handler.host, "port": self.handler.port}))
        super().__init__(**(kwargs | {"address": "localhost", "port": 7030}))
    """
    def _mount_graphql(self, app: web.Application):
        GraphQLView.attach(app, schema=schema, graphiql=True)
    """

    def register_handlers(self, app: web.Application):
        routes = [(r'/graphql', GraphqlHandler)]

        # if self.dev:
        routes += [(r'/graphiql', GraphiqlHandler)]

        app.router.add_routes([web.view(route[0], route[1]) for route in routes])

    async def initialize_jinja2(self, app: web.Application):
        aiohttp_jinja2.setup(app, loader=jinja2.FileSystemLoader('./templates'))

    async def create_application(self) -> web.Application:
        """Create the web application.

        :return: A ``web.Application`` instance.
        """
        app = web.Application()
        self.register_handlers(app)
        await self.initialize_jinja2(app)
        # self._mount_routes(app)
        #self._mount_graphql(app)
        return app
        # return self.handler.get_app()  # pragma: no cover
