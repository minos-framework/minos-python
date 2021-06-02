"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from abc import (
    ABC,
    abstractmethod,
)
from typing import (
    Any,
)

from .model import (
    MinosModel,
)


class Request(ABC):
    """Request interface."""

    @abstractmethod
    async def content(self) -> list[Any]:
        """Get the request content.

        :return: A list of instances.
        """


class Response:
    """Response definition."""

    __slots__ = "_items"

    def __init__(self, items: Any):
        if not isinstance(items, list):
            items = [items]
        self._items = items

    async def content(self) -> list:
        """Response content.

        :return: A list of items.
        """
        return self._items

    async def raw_content(self) -> list:
        """Raw response content.

        :return: A list of raw items.
        """
        return [item if not isinstance(item, MinosModel) else item.avro_data for item in self._items]
