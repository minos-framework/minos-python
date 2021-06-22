import socket
from typing import (
    NoReturn,
)

from minos.common import (
    MinosSetup,
)
from minos.networks.discovery_subscriber.discovery_client import (
    DiscoveryClient,
)


class DiscoverySubscriber(MinosSetup):
    async def _setup(self) -> NoReturn:
        ip = socket.gethostbyname(socket.getfqdn())
        port = 8080
        name = socket.gethostname()

        discovery_client = DiscoveryClient()
        await discovery_client.subscribe(ip, port, name)

    async def _destroy(self) -> NoReturn:
        name = socket.gethostname()

        discovery_client = DiscoveryClient()
        await discovery_client.unsubscribe(name)
