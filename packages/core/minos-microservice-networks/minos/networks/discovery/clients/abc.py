from __future__ import (
    annotations,
)

import logging
from abc import (
    ABC,
    abstractmethod,
)

from minos.common import (
    Config,
    SetupMixin,
)

logger = logging.getLogger(__name__)


class DiscoveryClient(ABC, SetupMixin):
    """Discovery Client class."""

    def __init__(self, host: str, port: int, **kwargs):
        super().__init__(**kwargs)
        self.host = host
        self.port = port

    @property
    def route(self) -> str:
        """Get the full http route to the discovery.

        :return: An ``str`` value.
        """
        # noinspection HttpUrlsUsage
        return f"http://{self.host}:{self.port}"

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> DiscoveryClient:
        discovery_config = config.get_discovery()

        client_host = discovery_config.get("host")
        client_port = discovery_config.get("port")
        return cls(host=client_host, port=client_port, **kwargs)

    @abstractmethod
    async def subscribe(
        self, host: str, port: int, name: str, endpoints: list[dict[str, str]], *args, **kwargs
    ) -> None:
        """Subscribe to the discovery.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :param endpoints: List of endpoints exposed by the microservice.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

    @abstractmethod
    async def unsubscribe(self, name: str, *args, **kwargs) -> None:
        """Unsubscribe from the discovery.

        :param name: The name of the microservice to be unsubscribed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
