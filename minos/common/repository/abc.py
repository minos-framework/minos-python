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

    def insert(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new insertion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = self._enrich(entry)
        entry.action = MinosRepositoryAction.INSERT
        self._submit(entry)

    def update(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new update entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = self._enrich(entry)
        entry.action = MinosRepositoryAction.UPDATE
        self._submit(entry)

    def delete(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new deletion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        entry = self._enrich(entry)
        entry.action = MinosRepositoryAction.DELETE
        self._submit(entry)

    def _enrich(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> MinosRepositoryEntry:
        if not isinstance(entry, MinosRepositoryEntry):
            aggregate = entry
            namespace = aggregate.get_namespace()
            if aggregate.id == 0:
                aggregate.id = self._generate_next_aggregate_id(namespace)
            aggregate.version = self._get_next_version_id(namespace, aggregate.id)
            entry = MinosRepositoryEntry.from_aggregate(aggregate)
        entry.id = self._generate_next_id()
        return entry

    @abstractmethod
    def _generate_next_id(self) -> NoReturn:
        ...

    @abstractmethod
    def _generate_next_aggregate_id(self, aggregate_name: str) -> int:
        """Generate a new unique id for the given aggregate name.

        :param aggregate_name: The name of the aggregate.
        :return: A positive-integer value.
        """

    @abstractmethod
    def _get_next_version_id(self, aggregate_name: str, aggregate_id: int) -> int:
        """Generate a new version number for the given aggregate name and identifier.

        :param aggregate_name: The name of the aggregate.
        :param aggregate_id: The identifier of the aggregate.
        :return: A positive-integer value.
        """

    @abstractmethod
    def _submit(self, entry: MinosRepositoryEntry) -> NoReturn:
        """Submit a new entry into the events table.

        :param entry: Entry to be submitted.
        :return: This method does not return anything.
        """

    @abstractmethod
    def select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A list of entries.
        """
