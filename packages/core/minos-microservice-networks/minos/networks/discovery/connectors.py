from __future__ import (
    annotations,
)

import logging
from inspect import (
    isclass,
)
from itertools import (
    chain,
)
from operator import (
    itemgetter,
)
from typing import (
    Any,
    Optional,
)

from minos.common import (
    Config,
    Injectable,
    SetupMixin,
)

from ..decorators import (
    EnrouteCollector,
)
from ..exceptions import (
    MinosInvalidDiscoveryClient,
)
from ..utils import (
    get_host_ip,
)
from .clients import (
    DiscoveryClient,
)

logger = logging.getLogger(__name__)


@Injectable("discovery")
class DiscoveryConnector(SetupMixin):
    """Discovery Connector class."""

    def __init__(
        self,
        client: DiscoveryClient,
        name: str,
        endpoints: list[dict[str, Any]],
        host: str,
        port: Optional[int] = None,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if port is None:
            port = 8080

        self.client = client

        self.name = name
        self.host = host
        self.port = port
        self.endpoints = endpoints

    @classmethod
    def _from_config(cls, *args, config: Config, **kwargs) -> DiscoveryConnector:
        client = cls._client_from_config(config)
        port = cls._port_from_config(config)
        name = config.get_name()
        host = get_host_ip()
        endpoints = cls._endpoints_from_config(config)

        return cls(client, name, endpoints, host, port, *args, **kwargs)

    @classmethod
    def _client_from_config(cls, config: Config) -> DiscoveryClient:
        discovery_config = config.get_discovery()

        client_cls = cls._client_cls_from_config(discovery_config)
        return client_cls.from_config(config)

    @staticmethod
    def _client_cls_from_config(discovery_config: dict[str, Any]) -> type[DiscoveryClient]:
        client_cls = discovery_config["client"]
        if not isclass(client_cls) or not issubclass(client_cls, DiscoveryClient):
            raise MinosInvalidDiscoveryClient(f"{client_cls!r} not supported.")
        return client_cls

    @staticmethod
    def _port_from_config(config: Config) -> Optional[int]:
        http_config = config.get_interface_by_name("http")
        connector_config = http_config["connector"]
        port = connector_config.get("port")
        return port

    @staticmethod
    def _endpoints_from_config(config: Config) -> list[dict[str, Any]]:
        endpoints = list()
        for name in config.get_services():
            decorators = EnrouteCollector(name, config).get_rest_command_query()
            endpoints += [
                {"url": decorator.url, "method": decorator.method} | decorator.kwargs
                for decorator in set(chain(*decorators.values()))
            ]

        endpoints.sort(key=itemgetter("url", "method"))
        return endpoints

    async def _setup(self) -> None:
        await self.subscribe()

    async def subscribe(self) -> None:
        """Send a subscribe operation to the discovery.

        :return: This method does not return anything.
        """
        logger.info("Performing discovery subscription...")
        await self.client.subscribe(self.host, self.port, self.name, self.endpoints)

    async def _destroy(self) -> None:
        await self.unsubscribe()

    async def unsubscribe(self) -> None:
        """Send an unsubscribe operation to the discovery.

        :return: This method does not return anything.
        """
        logger.info("Performing discovery unsubscription...")
        await self.client.unsubscribe(self.name)
