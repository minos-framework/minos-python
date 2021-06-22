"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    NoReturn,
)

import aiohttp

from ..exceptions import (
    MinosDiscoveryConnectorException,
)


class MinosDiscoveryClient:
    """Minos Discovery Client class."""

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

    async def subscribe(self, host: str, port: int, name: str) -> NoReturn:
        """Perform a subscription query.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/subscribe"
        service_metadata = {"ip": host, "port": port, "name": name}

        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint, json=service_metadata) as response:
                if not response.ok:
                    raise MinosDiscoveryConnectorException("There was a problem while trying to subscribe.")

    async def unsubscribe(self, name: str) -> NoReturn:
        """Perform an unsubscribe query.

        :param name: The name of the microservice to be unsubscribed.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/unsubscribe"
        query_param = f"?name={name}"

        async with aiohttp.ClientSession() as session:
            async with session.post(endpoint + query_param) as response:
                if not response.ok:
                    raise MinosDiscoveryConnectorException("There was a problem while trying to unsubscribe.")
