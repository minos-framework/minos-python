from aiohttp import (
    web,
)
from aiomisc.service.aiohttp import (
    AIOHTTPService,
)

from .handlers import (
    RestHandler,
)


class RestService(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, **kwargs):
        self.handler = RestHandler.from_config(**kwargs)
        super().__init__(**(kwargs | {"address": self.handler.host, "port": self.handler.port}))

    async def create_application(self) -> web.Application:
        """Create the web application.

        :return: A ``web.Application`` instance.
        """
        return self.handler.get_app()  # pragma: no cover
