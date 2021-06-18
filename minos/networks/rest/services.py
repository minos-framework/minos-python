"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from cached_property import (
    cached_property,
)

from aiomisc import (
    bind_socket,
)
from aiomisc.service.aiohttp import (
    AIOHTTPService,
)

from .builders import (
    RestBuilder,
)


class RestService(AIOHTTPService):
    """
    Rest Interface

    Expose REST Interface handler using aiomisc AIOHTTPService.

    """

    def __init__(self, **kwargs):
        super().__init__(port=8080, **kwargs)

    @cached_property
    def builder(self):
        """TODO

        :return: TODO
        """
        builder = RestBuilder.from_config()
        self.socket = bind_socket(address=builder.address, port=builder.port, proto_name="http",)
        return builder

    async def create_application(self):
        return self.builder.get_app()  # pragma: no cover
