import logging
from collections.abc import (
    Iterable,
)
from functools import (
    partial,
)

from aiohttp import (
    ClientResponseError,
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

    def __init__(self, *args, circuit_breaker_exceptions: Iterable[type] = tuple(), **kwargs):
        super().__init__(*args, circuit_breaker_exceptions=(ClientResponseError, *circuit_breaker_exceptions), **kwargs)

    async def subscribe(
        self,
        host: str,
        port: int,
        name: str,
        endpoints: list[dict[str, str]],
        retry_tries: int = 3,
        retry_delay: float = 5,
    ) -> None:
        """Perform the subscription query.

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

        fn = partial(self._subscribe, endpoint, service_metadata)
        await self.with_circuit_breaker(fn)

    @staticmethod
    async def _subscribe(endpoint: str, service_metadata: dict[str, str]) -> None:
        logger.debug(f"Subscribing into {endpoint!r}...")

        async with ClientSession() as session:
            async with session.post(endpoint, json=service_metadata) as response:
                response.raise_for_status()

    async def unsubscribe(self, name: str, retry_tries: int = 3, retry_delay: float = 5) -> None:
        """Perform the unsubscription query.

        :param name: The name of the microservice to be unsubscribed.
        :param retry_tries: Number of attempts before raising a failure exception.
        :param retry_delay: Seconds to wait between attempts.
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
