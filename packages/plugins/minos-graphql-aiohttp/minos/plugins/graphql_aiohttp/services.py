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
        pass
        #self.handler = RestHandler.from_config(**kwargs)
        #super().__init__(**(kwargs | {"address": self.handler.host, "port": self.handler.port}))

    async def create_application(self) -> web.Application:
        """Create the web application.

        :return: A ``web.Application`` instance.
        """
        app = web.Application()

        GraphQLView.attach(app, schema=schema, graphiql=True)
        return app
        # return self.handler.get_app()  # pragma: no cover


async def main():
    web.run_app(RestService.create_application())


