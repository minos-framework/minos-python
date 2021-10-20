from __future__ import (
    annotations,
)

from abc import (
    ABC,
    abstractmethod,
)
from asyncio import (
    gather,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from ..configuration import (
    MinosConfig,
)
from ..exceptions import (
    MinosBrokerNotProvidedException,
)
from ..networks import (
    MinosBroker,
)
from ..setup import (
    MinosSetup,
)
from .entries import (
    RepositoryEntry,
)

if TYPE_CHECKING:
    from ..model import (
        AggregateDiff,
    )


class MinosRepository(ABC, MinosSetup):
    """Base repository class in ``minos``."""

    @inject
    def __init__(self, event_broker: MinosBroker = Provide["event_broker"], *args, **kwargs):
        super().__init__(*args, **kwargs)
        if event_broker is None or isinstance(event_broker, Provide):
            raise MinosBrokerNotProvidedException("A broker instance is required.")
        self._broker = event_broker

    @classmethod
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> Optional[MinosRepository]:
        return cls(*args, **config.repository._asdict(), **kwargs)

    async def create(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new creation entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        from ..model import (
            Action,
            AggregateDiff,
        )

        aggregate_diff = None
        if isinstance(entry, AggregateDiff):
            aggregate_diff = entry
            entry = RepositoryEntry.from_aggregate_diff(entry)

        entry.action = Action.CREATE
        entry = await self._submit(entry)

        if aggregate_diff is not None:
            aggregate_diff.update_from_repository_entry(entry)
            await self._broker.send(aggregate_diff, topic=f"{aggregate_diff.simplified_name}Created")

        return entry

    async def update(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new update entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        from ..model import (
            Action,
            AggregateDiff,
        )

        aggregate_diff = None
        if isinstance(entry, AggregateDiff):
            aggregate_diff = entry
            entry = RepositoryEntry.from_aggregate_diff(entry)

        entry.action = Action.UPDATE
        entry = await self._submit(entry)

        if aggregate_diff is not None:
            aggregate_diff.update_from_repository_entry(entry)
            await self._send_update_events(aggregate_diff)

        return entry

    async def _send_update_events(self, aggregate_diff: AggregateDiff):
        from ..model import (
            IncrementalFieldDiff,
        )

        futures = [self._broker.send(aggregate_diff, topic=f"{aggregate_diff.simplified_name}Updated")]

        for decomposed_aggregate_diff in aggregate_diff.decompose():
            diff = next(iter(decomposed_aggregate_diff.fields_diff.flatten_values()))
            topic = f"{aggregate_diff.simplified_name}Updated.{diff.name}"
            if isinstance(diff, IncrementalFieldDiff):
                topic += f".{diff.action.value}"
            futures.append(self._broker.send(decomposed_aggregate_diff, topic=topic))

        await gather(*futures)

    async def delete(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new deletion entry into de repository.

        :param entry: Entry to be stored.
        :return: This method does not return anything.
        """
        from ..model import (
            Action,
            AggregateDiff,
        )

        aggregate_diff = None
        if isinstance(entry, AggregateDiff):
            aggregate_diff = entry
            entry = RepositoryEntry.from_aggregate_diff(entry)

        entry.action = Action.DELETE
        entry = await self._submit(entry)

        if aggregate_diff is not None:
            aggregate_diff.update_from_repository_entry(entry)
            await self._broker.send(aggregate_diff, topic=f"{aggregate_diff.simplified_name}Deleted")

        return entry

    @abstractmethod
    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        """Submit a new entry into the events table.

        :param entry: Entry to be submitted.
        :return: This method does not return anything.
        """

    # noinspection PyShadowingBuiltins
    async def select(
        self,
        aggregate_uuid: Optional[UUID] = None,
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
        **kwargs,
    ) -> AsyncIterator[RepositoryEntry]:
        """Perform a selection query of entries stored in to the repository.

        :param aggregate_uuid: Aggregate identifier.
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
        generator = self._select(
            aggregate_uuid=aggregate_uuid,
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
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, *args, **kwargs) -> AsyncIterator[RepositoryEntry]:
        """Perform a selection query of entries stored in to the repository."""
