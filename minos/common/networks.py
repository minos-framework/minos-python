"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import abc
from typing import (
    NoReturn,
)

from .model import (
    Aggregate,
)


class MinosBaseBroker(abc.ABC):
    """Base Broker class."""

    def __init__(self, topic: str):
        self.topic = topic

    @abc.abstractmethod
    async def send(self, items: list[Aggregate]) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :return: This method does not return anything.
        """
        raise NotImplementedError

    async def send_one(self, item: Aggregate) -> NoReturn:
        """Send one ``Aggregate`` instance.

        :param item: The instance to be send.
        :return: This method does not return anything.
        """
        return await self.send([item])
