import logging
from collections.abc import (
    Iterable,
)
from functools import (
    partial,
)
from typing import (
    Optional,
)

from aiohttp import (
    ClientError,
    ClientSession,
)

from minos.common import (
    CircuitBreakerMixin,
)
from minos.networks import (
    DiscoveryClient,
)

logger = logging.getLogger(__name__)


class MinosDiscoveryClient(DiscoveryClient, CircuitBreakerMixin):
    """Minos Discovery Client class."""

    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        circuit_breaker_exceptions: Iterable[type] = tuple(),
        **kwargs,
    ):
        if host is None:
            host = "localhost"
        if port is None:
            port = 5567
        super().__init__(host, port, circuit_breaker_exceptions=(ClientError, *circuit_breaker_exceptions), **kwargs)

    async def subscribe(
        self, host: str, port: int, name: str, endpoints: list[dict[str, str]], *args, **kwargs
    ) -> None:
        """Perform the subscription query.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :param endpoints: List of endpoints exposed by the microservice.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/microservices/{name}"
        service_metadata = {
            "address": host,
            "port": port,
            "endpoints": [[endpoint["method"], endpoint["url"]] for endpoint in endpoints],
        }

        fn = partial(self._subscribe, endpoint, service_metadata)
        await self.with_circuit_breaker(fn)

    @staticmethod
    async def _subscribe(endpoint: str, service_metadata: dict[str, str]) -> None:
        logger.debug(f"Subscribing into {endpoint!r}...")

        async with ClientSession() as session:
            async with session.post(endpoint, json=service_metadata) as response:
                response.raise_for_status()

    async def unsubscribe(self, name: str, *args, **kwargs) -> None:
        """Perform the unsubscription query.

        :param name: The name of the microservice to be unsubscribed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/microservices/{name}"

        fn = partial(self._unsubscribe, endpoint)
        await self.with_circuit_breaker(fn)

    @staticmethod
    async def _unsubscribe(endpoint: str) -> None:
        logger.debug(f"Unsubscribing into {endpoint!r}...")

        async with ClientSession() as session:
            async with session.delete(endpoint) as response:
                response.raise_for_status()
