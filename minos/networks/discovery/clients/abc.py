import logging
from abc import ABC, abstractmethod
from asyncio import sleep

import aiohttp

from ...exceptions import MinosDiscoveryConnectorException

logger = logging.getLogger(__name__)


class DiscoveryClient(ABC):
    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port

    @property
    def route(self) -> str:
        """Get the full http route to the repository.

        :return: An ``str`` value.
        """
        # noinspection HttpUrlsUsage
        return f"http://{self.host}:{self.port}"

    @abstractmethod
    async def subscribe(
        self,
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        retry_tries: int = 3,
        retry_delay: float = 5,
    ) -> None:
        pass

    async def _rest_subscribe(
        self,
        endpoint: str,
        service_metadata: dict[str, str],
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        retry_tries: int = 3,
        retry_delay: float = 5,
    ) -> None:
        logger.debug(f"Subscribing into {endpoint!r}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=service_metadata) as response:
                    success = response.ok
        except Exception as exc:
            logger.warning(f"An exception was raised while trying to subscribe: {exc!r}")
            success = False

        if not success:
            if retry_tries > 1:
                await sleep(retry_delay)
                return await self.subscribe(host, port, name, endpoints, retry_tries - 1, retry_delay)
            else:
                raise MinosDiscoveryConnectorException("There was a problem while trying to subscribe.")

    @abstractmethod
    async def unsubscribe(self, name: str, retry_tries: int = 3, retry_delay: float = 5) -> None:
        pass
