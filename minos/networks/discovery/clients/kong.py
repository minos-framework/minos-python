from .abc import (
    DiscoveryClient,
)


class KongDiscoveryClient(DiscoveryClient):
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
        endpoint = f"{self.route}/services"
        service_metadata = {
            "name": name,
            "url": f"http://{host}:{port}",
        }
        await self._rest_subscribe(endpoint, service_metadata, host, port, name, endpoints, retry_tries, retry_delay)

        endpoint = f"{self.route}/{name}/routes"
        service_metadata = {
            "paths": [endpoint["url"] for endpoint in endpoints],
        }
        # TODO Should we use _rest_subscribe here?
        await self._rest_subscribe(endpoint, service_metadata, host, port, name, endpoints, retry_tries, retry_delay)

    async def unsubscribe(self, name: str, retry_tries: int = 3, retry_delay: float = 5) -> None:
        """ Perform an unsubscribe query.

        :param name: The name of the microservice to be unsubscribed.
        :param retry_tries: Number of attempts before raising a failure exception.
        :param retry_delay: Seconds to wait between attempts.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/services/{name}"

        await self._rest_unsubscribe(endpoint, name, retry_tries, retry_delay)
