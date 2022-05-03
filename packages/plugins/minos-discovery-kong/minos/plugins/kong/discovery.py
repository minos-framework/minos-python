from __future__ import (
    annotations,
)

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
    Config,
)
from minos.networks import (
    DiscoveryClient,
)

from .client import (
    KongClient,
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
        client: KongClient = None,
        **kwargs,
    ):
        if host is None:
            host = "localhost"
        if port is None:
            port = 5567
        super().__init__(
            host, port, circuit_breaker_exceptions=(httpx.HTTPStatusError, *circuit_breaker_exceptions), **kwargs
        )

        if client is None:
            client = KongClient(host=host, port=port)

        self.client = client

        self.auth_type = None
        if "auth_type" in kwargs:
            self.auth_type = kwargs["auth_type"]

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> KongDiscoveryClient:
        if "auth_type" in kwargs:
            auth_type = kwargs["auth_type"]
            kwargs.pop("auth_type")
        else:
            try:
                auth_type = config.get_by_key("discovery.auth-type")
            except Exception:
                auth_type = None
        client = KongClient.from_config(config)

        return super()._from_config(config, auth_type=auth_type, client=client, **kwargs)

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

        fnsr = partial(self.client.register_service, self.route, name, host, port)
        response_service = await self.with_circuit_breaker(fnsr)  # register a service
        if response_service.status_code == 409:  # service already exist
            # if service already exist, delete and add again
            fn_delete = partial(self.client.delete_service, self.route, name)
            await self.with_circuit_breaker(fn_delete)  # delete the service
            fnsr = partial(self.client.register_service, self.route, name, host, port)
            response_service = await self.with_circuit_breaker(fnsr)  # send the servie subscription again

        content_service = response_service.json()  # get the servie information like the id

        for endpoint in endpoints:
            endpointClass = Endpoint(endpoint["url"])

            regex_priority = 0
            if "regex_priority" in endpoint:
                regex_priority = endpoint["regex_priority"]

            fn = partial(
                self.client.create_route,
                self.route,
                ["http"],
                [endpoint["method"]],
                [endpointClass.path_as_regex],
                content_service["id"],
                regex_priority,
                False,
            )
            response = await self.with_circuit_breaker(fn)  # send the route request
            resp = response.json()

            if "authenticated" in endpoint and self.auth_type:
                if self.auth_type == "basic-auth":
                    fn = partial(self.client.activate_basic_auth_plugin_on_route, route_id=resp["id"])
                    await self.with_circuit_breaker(fn)
                elif self.auth_type == "jwt":
                    fn = partial(self.client.activate_jwt_plugin_on_route, route_id=resp["id"])
                    await self.with_circuit_breaker(fn)

            if "authorized_groups" in endpoint:
                fn = partial(
                    self.client.activate_acl_plugin_on_route, route_id=resp["id"], allow=endpoint["authorized_groups"]
                )
                await self.with_circuit_breaker(fn)

        return response

    async def unsubscribe(self, name: str, *args, **kwargs) -> httpx.Response:
        """Perform the unsubscription query.

        :param name: The name of the microservice to be unsubscribed.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        fn = partial(self.client.delete_service, self.route, name)
        response = await self.with_circuit_breaker(fn)
        return response
