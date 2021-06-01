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


class Request(ABC):
    """Request interface."""

    @abstractmethod
    async def content(self) -> list[Any]:
        """Get the request content.

        :return: A list of instances.
        """


class Response(ABC):
    """Response definition."""

    @abstractmethod
    async def content(self) -> list[Any]:
        """Get the response content.

        :return: A list of instances.
        """
