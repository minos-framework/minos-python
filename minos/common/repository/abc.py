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
    Union,
)

from .entries import (
    MinosRepositoryAction,
    MinosRepositoryEntry,
)

if TYPE_CHECKING:
    from ..model import Aggregate


class MinosRepository(ABC):
    """Base repository class in ``minos``."""

    async def __aenter__(self) -> MinosRepository:
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        pass

    async def setup(self):
        """TODO

        :return: TODO
        """
        pass

    async def insert(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new insertion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        if not isinstance(entry, MinosRepositoryEntry):
            entry = MinosRepositoryEntry.from_aggregate(entry)

        entry.action = MinosRepositoryAction.INSERT
        return await self._submit(entry)

    async def update(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new update entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        if not isinstance(entry, MinosRepositoryEntry):
            entry = MinosRepositoryEntry.from_aggregate(entry)

        entry.action = MinosRepositoryAction.UPDATE
        return await self._submit(entry)

    async def delete(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new deletion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        if not isinstance(entry, MinosRepositoryEntry):
            entry = MinosRepositoryEntry.from_aggregate(entry)

        entry.action = MinosRepositoryAction.DELETE
        return await self._submit(entry)

    @abstractmethod
    async def _submit(self, entry: MinosRepositoryEntry) -> MinosRepositoryEntry:
        """Submit a new entry into the events table.

        :param entry: Entry to be submitted.
        :return: This method does not return anything.
        """

    @abstractmethod
    async def select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A list of entries.
        """
