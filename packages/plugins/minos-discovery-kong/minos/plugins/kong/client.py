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

import httpx as httpx

from minos.common import (
    CircuitBreakerMixin,
)
from minos.networks import (
    DiscoveryClient,
)

from .utils import (
    Endpoint,
)

logger = logging.getLogger(__name__)


class KongDiscoveryClient(DiscoveryClient, CircuitBreakerMixin):
    """Kong Discovery Client class."""

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
        super().__init__(
            host, port, circuit_breaker_exceptions=(httpx.HTTPStatusError, *circuit_breaker_exceptions), **kwargs
        )

    async def subscribe(
        self, host: str, port: int, name: str, endpoints: list[dict[str, str]], *args, **kwargs
    ) -> httpx.Response:
        """Perform the subscription query.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :param endpoints: List of endpoints exposed by the microservice.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """

        fnsr = partial(self._register_service, self.route, name, host, port)
        response_service = await self.with_circuit_breaker(fnsr)  # register a service
        if response_service.status_code == 409:  # service already exist
            # if service already exist, delete and add again
            fn_delete = partial(self._delete_service, self.route, name)
            await self.with_circuit_breaker(fn_delete)  # delete the service
            fnsr = partial(self._register_service, self.route, name, host, port)
            response_service = await self.with_circuit_breaker(fnsr)  # send the servie subscription again

        content_service = response_service.json()  # get the servie information like the id

        for endpoint in endpoints:
            endpointClass = Endpoint(endpoint["url"])
            data = {
                "protocols": ["http"],
                "methods": [endpoint["method"]],
                "paths": [endpointClass.path_as_str],
                "service": {"id": content_service["id"]},
                "strip_path": False,
            }
            fn = partial(self._subscribe_routes, self.route, data)
            response = await self.with_circuit_breaker(fn)  # send the route request

        return response

    @staticmethod
    async def _register_service(
        discovery_route: str, service_name: str, microservice_host: str, microservice_port: str
    ) -> httpx.Response:
        url = f"{discovery_route}/services"  # kong url for service POST or add
        data = {"name": service_name, "protocol": "http", "host": microservice_host, "port": microservice_port}
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data)
            return response

    @staticmethod
    async def _subscribe_routes(endpoint: str, data: dict[str, str]) -> httpx.Response:
        logger.debug(f"Subscribing into {endpoint!r}...")
        url = f"{endpoint}/routes"
        async with httpx.AsyncClient() as client:
            response = await client.post(url, json=data)
            return response

    async def unsubscribe(self, name: str, *args, **kwargs) -> httpx.Response:
        """Perform the unsubscription query.

        :param name: The name of the microservice to be unsubscribed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        fn = partial(self._delete_service, self.route, name)
        response = await self.with_circuit_breaker(fn)
        return response

    @staticmethod
    async def _delete_service(discovery_route: str, service_name) -> httpx.Response:
        """
        the delete of a service must be checking before if the service already have the routes
        if yes the DELETE routes must be called
        :param discovery_route:
        :param service_name:
        :return:
        """
        async with httpx.AsyncClient() as client:
            url_get_route = f"{discovery_route}/services/{service_name}/routes"  # url to get the routes
            response_routes = await client.get(url_get_route)
            json_routes_response = response_routes.json()
            if len(json_routes_response["data"]) > 0:  # service already have route, routes must be deleted
                for route in json_routes_response["data"]:
                    url_delete_route = f"{discovery_route}/routes/{route['id']}"  # url for routes delete
                    await client.delete(url_delete_route)
            url_delete_service = f"{discovery_route}/services/{service_name}"  # url for service delete
            response_delete_service = await client.delete(url_delete_service)
            return response_delete_service
