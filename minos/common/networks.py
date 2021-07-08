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
