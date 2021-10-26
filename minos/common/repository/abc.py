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
    Awaitable,
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
    MinosRepositoryConflictException,
    MinosRepositoryException,
)
from ..networks import (
    MinosBroker,
)
from ..setup import (
    MinosSetup,
)
from ..transactions import (
    TRANSACTION_CONTEXT_VAR,
    Transaction,
    TransactionRepository,
    TransactionStatus,
)
from ..uuid import (
    NULL_UUID,
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
    def __init__(
        self,
        event_broker: MinosBroker = Provide["event_broker"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if event_broker is None or isinstance(event_broker, Provide):
            raise MinosBrokerNotProvidedException("A broker instance is required.")

        self._broker = event_broker
        self._transaction_repository = transaction_repository

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

    async def _check_transaction(self, transaction: Transaction) -> bool:
        # FIXME: This check must be within a lock.

        async for entry in self.select(transaction_uuid=transaction.uuid):
            async for _ in self.select(
                aggregate_uuid=entry.aggregate_uuid, version=entry.version, transaction_uuid=NULL_UUID
            ):
                return False

            transaction_uuids = {
                e.transaction_uuid
                async for e in self.select(aggregate_uuid=entry.aggregate_uuid, version=entry.version)
            }

            async for _ in self._transaction_repository.select(
                uuid_in=tuple(transaction_uuids), status=TransactionStatus.RESERVED
            ):
                return False

        return True

    @property
    def offset(self) -> Awaitable[int]:
        """TODO"""
        return self._offset

    @property
    @abstractmethod
    async def _offset(self) -> int:
        raise NotImplementedError

    async def _commit_transaction(self, transaction: Transaction) -> int:
        """TODO

        :param transaction: TODO
        :return: TODO
        """
        entries = list()
        async for entry in self.select(transaction_uuid=transaction.uuid):
            new = RepositoryEntry.from_another(entry, transaction_uuid=NULL_UUID)
            committed = await self.submit(new, _transaction_uuid=transaction.uuid)
            entries.append(committed)
        return max(e.id for e in entries)

    async def submit(
        self, entry: Union[AggregateDiff, RepositoryEntry], _transaction_uuid: Optional[UUID] = None
    ) -> RepositoryEntry:
        """Store new entry into the repository.

        :param entry: The entry to be stored.
        :return: The repository entry containing the stored information.
        """
        from ..model import (
            Action,
            AggregateDiff,
        )

        transaction = self._transaction

        if isinstance(entry, AggregateDiff):
            entry = RepositoryEntry.from_aggregate_diff(entry, transaction=transaction)

        if not isinstance(entry.action, Action):
            raise MinosRepositoryException("The 'RepositoryEntry.action' attribute must be an 'Action' instance.")

        if transaction is None and isinstance(self._transaction_repository, TransactionRepository):
            transaction_uuids = {
                e.transaction_uuid
                async for e in self.select(aggregate_uuid=entry.aggregate_uuid)
                if e.transaction_uuid != _transaction_uuid
            }
            if len(transaction_uuids):
                async for transaction in self._transaction_repository.select(
                    uuid_in=tuple(transaction_uuids), status=TransactionStatus.RESERVED
                ):
                    raise MinosRepositoryConflictException(
                        f"The {transaction!r} transaction has already reserved the (uuid, version) key: {entry!r}",
                        transaction.event_offset,
                    )

        entry = await self._submit(entry)

        if transaction is None:
            await self._send_events(entry.aggregate_diff)
        else:
            await transaction.save(event_offset=entry.id, status=TransactionStatus.PENDING)

        return entry

    @property
    def _transaction(self) -> Optional[Transaction]:
        return TRANSACTION_CONTEXT_VAR.get()

    def begin(self, **kwargs) -> Transaction:
        """TODO

        :param kwargs: TODO
        :return: TODO
        """
        return Transaction(event_repository=self, transaction_repository=self._transaction_repository, **kwargs)

    @abstractmethod
    async def _submit(self, entry: RepositoryEntry) -> RepositoryEntry:
        raise NotImplementedError

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
        :param transaction_uuid: Transaction identifier.
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
