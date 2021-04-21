"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

from __future__ import annotations

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

    def __enter__(self) -> MinosRepository:
        return self

    def __exit__(self, exc_type, exc_value, exc_traceback):
        pass

    async def insert(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new insertion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = await self._enrich(entry)
        entry.action = MinosRepositoryAction.INSERT
        await self._submit(entry)

    async def update(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new update entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = await self._enrich(entry)
        entry.action = MinosRepositoryAction.UPDATE
        await self._submit(entry)

    async def delete(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new deletion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = await self._enrich(entry)
        entry.action = MinosRepositoryAction.DELETE
        await self._submit(entry)

    async def _enrich(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> MinosRepositoryEntry:
        if not isinstance(entry, MinosRepositoryEntry):
            aggregate = entry
            namespace = aggregate.get_namespace()
            if aggregate.id == 0:
                aggregate.id = await self._generate_next_aggregate_id(namespace)
            aggregate.version = await self._get_next_version_id(namespace, aggregate.id)
            entry = MinosRepositoryEntry.from_aggregate(aggregate)
        entry.id = await self._generate_next_id()
        return entry

    @abstractmethod
    async def _generate_next_id(self) -> int:
        """Generate next id to be used for a new entry.

        :return: A positive-integer value.
        """

    @abstractmethod
    async def _generate_next_aggregate_id(self, aggregate_name: str) -> int:
        """Generate a new unique id for the given aggregate name.

        :param aggregate_name: The name of the aggregate.
        :return: A positive-integer value.
        """

    @abstractmethod
    async def _get_next_version_id(self, aggregate_name: str, aggregate_id: int) -> int:
        """Generate a new version number for the given aggregate name and identifier.

        :param aggregate_name: The name of the aggregate.
        :param aggregate_id: The identifier of the aggregate.
        :return: A positive-integer value.
        """

    @abstractmethod
    async def _submit(self, entry: MinosRepositoryEntry) -> NoReturn:
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
