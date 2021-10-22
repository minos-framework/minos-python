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
from ..uuid import (
    NULL_UUID,
)
from .entries import (
    RepositoryEntry,
)
from .transactions import (
    TRANSACTION_CONTEXT_VAR,
    RepositoryTransaction,
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
        """Store new creation entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ..model import (
            Action,
        )

        entry.action = Action.CREATE
        return await self.submit(entry)

    async def update(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new update entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ..model import (
            Action,
        )

        entry.action = Action.UPDATE
        return await self.submit(entry)

    async def delete(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new deletion entry into the repository.

        :param entry: Entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ..model import (
            Action,
        )

        entry.action = Action.DELETE
        return await self.submit(entry)

    async def commit(self, transaction: RepositoryTransaction) -> list[RepositoryEntry]:
        """TODO

        :param transaction: TODO
        :return: TODO
        """
        entries = list()
        async for entry in self.select(transaction_uuid=transaction.uuid):
            new = RepositoryEntry(
                aggregate_uuid=entry.aggregate_uuid,
                aggregate_name=entry.aggregate_name,
                version=entry.version,
                data=entry.data,
                action=entry.action,
                created_at=entry.created_at,
            )
            committed = await self.submit(new)
            entries.append(committed)

        return entries

    async def submit(self, entry: Union[AggregateDiff, RepositoryEntry]) -> RepositoryEntry:
        """Store new entry into the repository.

        :param entry: The entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ..model import (
            AggregateDiff,
        )

        if isinstance(entry, AggregateDiff):
            entry = RepositoryEntry.from_aggregate_diff(entry, transaction_uuid=self._transaction_uuid)

        entry = await self._submit(entry)

        transaction = TRANSACTION_CONTEXT_VAR.get()
        if transaction is not None:
            transaction.entries.append(entry)

        if entry.transaction_uuid == NULL_UUID:
            await self._send_events(entry.aggregate_diff)

        return entry

    @property
    def _transaction_uuid(self) -> UUID:
        transaction = TRANSACTION_CONTEXT_VAR.get()
        if transaction is None:
            return NULL_UUID
        return transaction.uuid

    def begin(self, **kwargs) -> RepositoryTransaction:
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        return RepositoryTransaction(self, **kwargs)

    @abstractmethod
    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        """Submit a new entry into the events table.

        :param entry: Entry to be submitted.
        :return: This method does not return anything.
        """

    async def _send_events(self, aggregate_diff: AggregateDiff):
        from ..model import (
            Action,
        )

        suffix_mapper = {
            Action.CREATE: "Created",
            Action.UPDATE: "Updated",
            Action.DELETE: "Deleted",
        }

        topic = f"{aggregate_diff.simplified_name}{suffix_mapper[aggregate_diff.action]}"
        futures = [self._broker.send(aggregate_diff, topic=topic)]

        if aggregate_diff.action == Action.UPDATE:
            from ..model import (
                IncrementalFieldDiff,
            )

            for decomposed_aggregate_diff in aggregate_diff.decompose():
                diff = next(iter(decomposed_aggregate_diff.fields_diff.flatten_values()))
                composed_topic = f"{topic}.{diff.name}"
                if isinstance(diff, IncrementalFieldDiff):
                    composed_topic += f".{diff.action.value}"
                futures.append(self._broker.send(decomposed_aggregate_diff, topic=composed_topic))

        await gather(*futures)

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
        transaction_uuid: Optional[UUID] = None,
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
        :param transaction_uuid: TODO
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
            transaction_uuid=transaction_uuid,
            **kwargs,
        )
        # noinspection PyTypeChecker
        async for entry in generator:
            yield entry

    @abstractmethod
    async def _select(self, *args, **kwargs) -> AsyncIterator[RepositoryEntry]:
        """Perform a selection query of entries stored in to the repository."""
