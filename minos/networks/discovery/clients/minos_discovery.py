"""minos.networks.discovery.clients module."""

import logging
from asyncio import (
    sleep,
)
from typing import (
    NoReturn,
)

import aiohttp

from .abc import DiscoveryClient
from ...exceptions import (
    MinosDiscoveryConnectorException,
)

logger = logging.getLogger(__name__)


class MinosDiscoveryClient(DiscoveryClient):
    """Minos Discovery Client class."""

    async def subscribe(
        self,
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        retry_tries: int = 3,
        retry_delay: float = 5,
    ) -> None:
        """Perform a subscription query.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :param endpoints: List of endpoints exposed by the microservice.
        :param retry_tries: Number of attempts before raising a failure exception.
        :param retry_delay: Seconds to wait between attempts.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/microservices/{name}"
        service_metadata = {
            "address": host,
            "port": port,
            "endpoints": [[endpoint["method"], endpoint["url"]] for endpoint in endpoints],
        }

        await self._rest_subscribe(endpoint, service_metadata, host, port, name, endpoints, retry_tries, retry_delay)

    async def unsubscribe(self, name: str, retry_tries: int = 3, retry_delay: float = 5) -> NoReturn:
        """Perform an unsubscribe query.

        :param name: The name of the microservice to be unsubscribed.
        :param retry_tries: Number of attempts before raising a failure exception.
        :param retry_delay: Seconds to wait between attempts.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/microservices/{name}"

        logger.debug(f"Unsubscribing into {endpoint!r}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.delete(endpoint) as response:
                    success = response.ok
        except Exception as exc:
            logger.warning(f"An exception was raised while trying to unsubscribe: {exc!r}")
            success = False

        if not success:
            if retry_tries > 1:
                await sleep(retry_delay)
                return await self.unsubscribe(name, retry_tries - 1, retry_delay)
            else:
                raise MinosDiscoveryConnectorException("There was a problem while trying to unsubscribe.")
