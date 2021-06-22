"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from typing import (
    NoReturn,
)

import aiohttp


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

    async def subscribe(self, ip: str, port: int, name: str) -> NoReturn:
        """Perform a subscription query.

        :param ip: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/subscribe"
        async with aiohttp.ClientSession() as session:
            service_metadata = {"ip": ip, "port": port, "name": name}

            await session.post(endpoint, json=service_metadata)

    async def unsubscribe(self, name: str) -> NoReturn:
        """Perform an unsubscribe query.

        :param name: The name of the microservice to be unsubscribed.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/unsubscribe"
        async with aiohttp.ClientSession() as session:
            query_param = f"?name={name}"

            await session.post(endpoint + query_param)
