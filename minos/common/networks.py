"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    NoReturn,
)

from .model import (
    Aggregate,
)


class MinosBroker(ABC):
    """Base Broker class."""

    def __init__(self, topic: str):
        self.topic = topic

    async def send_one(self, item: Aggregate, **kwargs) -> NoReturn:
        """Send one ``Aggregate`` instance.

        :param item: The instance to be send.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.send([item], **kwargs)

    @abstractmethod
    async def send(self, items: list[Aggregate], **kwargs) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError
