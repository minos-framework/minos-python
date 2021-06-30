"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import logging
from asyncio import (
    sleep,
)
from typing import (
    NoReturn,
)

import aiohttp

from ..exceptions import (
    MinosDiscoveryConnectorException,
)

logger = logging.getLogger(__name__)


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

    async def subscribe(
        self, host: str, port: int, name: str, retry_attempts: int = 2, retry_interval: float = 5
    ) -> NoReturn:
        """Perform a subscription query.

        :param host: The ip of the microservice to be subscribed.
        :param port: The port of the microservice to be subscribed.
        :param name: The name of the microservice to be subscribed.
        :param retry_attempts: Number of attempts before raising a failure exception.
        :param retry_interval: Number of seconds to wait between attempts.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/subscribe"
        service_metadata = {"ip": host, "port": port, "name": name}

        logger.debug(f"Subscribing into {endpoint!r}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint, json=service_metadata) as response:
                    success = response.ok
        except Exception as exc:
            logger.warning(f"An exception was raised while trying to subscribe: {exc!r}")
            success = False

        if not success:
            if retry_attempts > 0:
                await sleep(retry_interval)
                return await self.subscribe(host, port, name, retry_attempts - 1, retry_interval)
            else:
                raise MinosDiscoveryConnectorException("There was a problem while trying to subscribe.")

    async def unsubscribe(self, name: str, retry_attempts: int = 2, retry_interval: float = 5) -> NoReturn:
        """Perform an unsubscribe query.

        :param name: The name of the microservice to be unsubscribed.
        :param retry_attempts: Number of attempts before raising a failure exception.
        :param retry_interval: Number of seconds to wait between attempts.
        :return: This method does not return anything.
        """
        endpoint = f"{self.route}/unsubscribe?name={name}"

        logger.debug(f"Unsubscribing into {endpoint!r}...")
        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(endpoint) as response:
                    success = response.ok
        except Exception as exc:
            logger.warning(f"An exception was raised while trying to unsubscribe: {exc!r}")
            success = False

        if not success:
            if retry_attempts > 0:
                await sleep(retry_interval)
                return await self.unsubscribe(name, retry_attempts - 1, retry_interval)
            else:
                raise MinosDiscoveryConnectorException("There was a problem while trying to unsubscribe.")
