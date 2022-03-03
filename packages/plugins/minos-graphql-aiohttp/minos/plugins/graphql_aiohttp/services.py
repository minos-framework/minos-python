from aiohttp import (
    web,
)
from aiomisc.service.aiohttp import (
    AIOHTTPService,
)
from .handlers import (
    RestHandler,
)
from graphql_server.aiohttp import GraphQLView

from .graphql_example import schema


class RestService(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, **kwargs):
        #self.handler = RestHandler.from_config(**kwargs)
        #super().__init__(**(kwargs | {"address": self.handler.host, "port": self.handler.port}))
        super().__init__(**(kwargs | {"address": "localhost", "port": 7030}))

    def _mount_graphql(self, app: web.Application):
        GraphQLView.attach(app, schema=schema, graphiql=True)

    async def create_application(self) -> web.Application:
        """Create the web application.

        :return: A ``web.Application`` instance.
        """
        app = web.Application()
        # self._mount_routes(app)
        self._mount_graphql(app)
        return app
        #return self.handler.get_app()  # pragma: no cover

