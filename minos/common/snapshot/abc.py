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
    AsyncIterator,
)
from uuid import (
    UUID,
)

from ..setup import (
    MinosSetup,
)
from .conditions import (
    Condition,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class MinosSnapshot(ABC, MinosSetup):
    """Base Snapshot class."""

    @abstractmethod
    async def get(self, aggregate_name: str, uuid: UUID, **kwargs) -> Aggregate:
        """TODO

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuid: Set of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """

    @abstractmethod
    async def find(self, aggregate_name: str, condition: Condition, **kwargs) -> AsyncIterator[Aggregate]:
        """TODO

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param condition: TODO
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
