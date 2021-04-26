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
    Optional,
    Union,
)

from ..configuration import (
    MinosConfig,
)
from .entries import (
    MinosRepositoryAction,
    MinosRepositoryEntry,
)

if TYPE_CHECKING:
    from ..model import Aggregate


class MinosRepository(ABC):
    """Base repository class in ``minos``."""

    def __init__(self, already_setup: bool = False, *args, **kwargs):
        self._already_setup = already_setup

    @classmethod
    def from_config(cls, *args, config: MinosConfig = None, **kwargs) -> Optional[MinosRepository]:
        """Build a new repository from config.

        :param args: Additional positional arguments.
        :param config: Config instance. If `None` is provided, default config is chosen.
        :param kwargs: Additional named arguments.
        :return: A `MinosRepository` instance.
        """
        if config is None:
            config = MinosConfig.get_default()
        if config is None:
            return None
        # noinspection PyProtectedMember
        return cls(*args, **config.repository._asdict(), **kwargs)

    async def __aenter__(self) -> MinosRepository:
        await self.setup()
        return self

    async def __aexit__(self, exc_type, exc_value, exc_traceback):
        pass

    async def insert(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new insertion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        await self.setup()

        if not isinstance(entry, MinosRepositoryEntry):
            entry = MinosRepositoryEntry.from_aggregate(entry)

        entry.action = MinosRepositoryAction.INSERT
        return await self._submit(entry)

    async def update(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new update entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        await self.setup()

        if not isinstance(entry, MinosRepositoryEntry):
            entry = MinosRepositoryEntry.from_aggregate(entry)

        entry.action = MinosRepositoryAction.UPDATE
        return await self._submit(entry)

    async def delete(self, entry: Union[Aggregate, MinosRepositoryEntry]) -> NoReturn:
        """Store new deletion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        await self.setup()

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

    # noinspection PyShadowingBuiltins
    async def select(
        self,
        aggregate_id: Optional[int] = None,
        aggregate_name: Optional[str] = None,
        version: Optional[int] = None,
        version_lt: Optional[int] = None,
        version_gt: Optional[int] = None,
        version_le: Optional[int] = None,
        version_ge: Optional[int] = None,
        id: Optional[int] = None,
        id_lt: Optional[int] = None,
        id_gt: Optional[int] = None,
        id_le: Optional[int] = None,
        id_ge: Optional[int] = None,
    ) -> list[MinosRepositoryEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param aggregate_id: Aggregate identifier.
        :param aggregate_name: Aggregate name.
        :param version: Aggregate version.
        :param version_lt: Aggregate version lower than the given value.
        :param version_gt: Aggregate version greater than the given value.
        :param version_le: Aggregate version lower or equal to the given value.
        :param version_ge: Aggregate version greater or equal to the given value.
        :param id: Entry identifier.
        :param id_lt: Entry identifier lower than the given value.
        :param id_gt: Entry identifier greater than the given value.
        :param id_le: Entry identifier lower or equal to the given value.
        :param id_ge: Entry identifier greater or equal to the given value.
        :return: A list of entries.
        """
        await self.setup()

        return await self._select(
            aggregate_id=aggregate_id,
            aggregate_name=aggregate_name,
            version=version,
            version_lt=version_lt,
            version_gt=version_gt,
            version_le=version_le,
            version_ge=version_ge,
            id=id,
            id_lt=id_lt,
            id_gt=id_gt,
            id_le=id_le,
            id_ge=id_ge,
        )

    @abstractmethod
    async def _select(self, *args, **kwargs) -> list[MinosRepositoryEntry]:
        """Perform a selection query of entries stored in to the repository."""

    async def setup(self) -> NoReturn:
        """Setup miscellaneous repository thing.

        :return: This method does not return anything.
        """
        if not self._already_setup:
            await self._setup()
            self._already_setup = True

    async def _setup(self) -> NoReturn:
        """Setup miscellaneous repository thing."""
