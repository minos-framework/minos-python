import typing as t

from aiomisc.service.aiohttp import (
    AIOHTTPService,
)

from minos.common import (
    MinosConfig,
)

from .handler import (
    RestInterfaceHandler,
)


class REST(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, config: MinosConfig, **kwds: t.Any):
        address = config.rest.broker.host
        port = config.rest.broker.port
        super().__init__(address=address, port=port, **kwds)
        self._config = config
        self.rest_interface = RestInterfaceHandler(config=self._config)

    async def create_application(self):
        return self.rest_interface.get_app()  # pragma: no cover
