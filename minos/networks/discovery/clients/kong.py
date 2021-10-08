import aiohttp

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

        endpoint = f"{self.route}/services/{name}/routes"
        service_metadata = {
            "paths": [endpoint["url"] for endpoint in endpoints],
            "methods": ["POST", "GET", "DELETE"],
            "strip_path": False,
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
        await self._delete_routes(self.route, name)
        await self._delete_service(self.route, name)

    async def _delete_routes(self, route: str, name: str) -> None:
        routes_ids = await self._get_routes(self.route, name)

        for route_id in routes_ids:
            endpoint = f"{route}/services/{name}/routes/{route_id}"

            async with aiohttp.ClientSession() as session:
                async with session.delete(endpoint) as response:
                    pass

    async def _get_routes(self, route: str, name: str) -> list[str]:
        endpoint = f"{route}/services/{name}/routes"

        routes_ids = []
        async with aiohttp.ClientSession() as session:
            async with session.get(endpoint) as response:
                routes = await response.json()
                routes_ids = [route["id"] for route in routes["data"]]

        return routes_ids

    async def _delete_service(self, route, name) -> None:
        endpoint = f"{route}/services/{name}"

        async with aiohttp.ClientSession() as session:
            async with session.delete(endpoint) as response:
                pass
