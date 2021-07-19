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
    Any,
    NoReturn,
)

from .setup import (
    MinosSetup,
)


class MinosBroker(ABC, MinosSetup):
    """Base Broker class."""

    @abstractmethod
    async def send(self, data: Any, **kwargs) -> NoReturn:
        """Send a list of ``Aggregate`` instances.

        :param data: The data to be send.
        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        raise NotImplementedError


class MinosHandler(ABC, MinosSetup):
    """Base Handler class."""

    @abstractmethod
    async def get_one(self, *args, **kwargs) -> Any:
        """Get one entry.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The entry to be retrieved.
        """
        raise NotImplementedError

    @abstractmethod
    async def get_many(self, *args, **kwargs) -> list[Any]:
        """Get a list of entries.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: The list of entries to be retrieved.
        """
        raise NotImplementedError
