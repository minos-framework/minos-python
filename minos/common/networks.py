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

if TYPE_CHECKING:
    from .model import (
        MinosModel,
    )


class MinosBroker(ABC):
    """Base Broker class."""

    @classmethod
    async def send_one(cls, item: MinosModel, **kwargs) -> NoReturn:
        """Send one ``Aggregate`` instance.

        :param item: The instance to be send.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        return await cls.send([item], **kwargs)

    @classmethod
    @abstractmethod
    async def send(cls, items: list[MinosModel], **kwargs) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param items: A list of aggregates.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError
