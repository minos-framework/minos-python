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
    Type,
)

from minos.common import (
    MinosConfig,
    MinosImportException,
    MinosSetup,
    import_module,
)

from ..decorators import (
    EnrouteAnalyzer,
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


class DiscoveryConnector(MinosSetup):
    """Discovery Connector class."""

    def __init__(self, client, name: str, host: str, port: int, endpoints: list[dict[str, Any]], *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.client = client

        self.name = name
        self.host = host
        self.port = port
        self.endpoints = endpoints

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> DiscoveryConnector:
        client_cls = cls._client_cls_from_config(config)
        client = client_cls(host=config.discovery.host, port=config.discovery.port)
        port = config.rest.port
        name = config.service.name
        host = get_host_ip()
        endpoints = cls._endpoints_from_config(config)

        return cls(client, name, host, port, endpoints, *args, **kwargs)

    @staticmethod
    def _client_cls_from_config(config: MinosConfig) -> Type[DiscoveryClient]:
        try:
            # noinspection PyTypeChecker
            client_cls: type = import_module(config.discovery.client)
        except MinosImportException:
            raise MinosInvalidDiscoveryClient(f"{config.discovery.client} could not be imported.")

        if not isclass(client_cls) or not issubclass(client_cls, DiscoveryClient):
            raise MinosInvalidDiscoveryClient(f"{config.discovery.client} not supported.")
        return client_cls

    @staticmethod
    def _endpoints_from_config(config: MinosConfig) -> list[dict[str, Any]]:
        command_decorators = EnrouteAnalyzer(config.commands.service, config).get_rest_command_query()
        query_decorators = EnrouteAnalyzer(config.queries.service, config).get_rest_command_query()

        endpoints = chain(chain(*command_decorators.values()), chain(*query_decorators.values()))
        endpoints = set(endpoints)
        endpoints = [{"url": decorator.url, "method": decorator.method} for decorator in endpoints]
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
