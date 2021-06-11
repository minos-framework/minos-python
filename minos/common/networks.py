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
    TYPE_CHECKING,
    NoReturn,
)

from .setup import (
    MinosSetup,
)

if TYPE_CHECKING:
    from .model import (
        Model,
    )


class MinosBroker(ABC, MinosSetup):
    """Base Broker class."""

    async def send_one(self, item: Model, **kwargs) -> NoReturn:
        """Send one ``Aggregate`` instance.

        :param item: The instance to be send.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await self.send([item], **kwargs)

    @abstractmethod
    async def send(self, items: list[Model], **kwargs) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError
