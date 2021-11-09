from __future__ import (
    annotations,
)

from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from dependency_injector.wiring import (
    Provide,
    inject,
)

from minos.common import (
    NULL_UUID,
    NotProvidedException,
)

from ..events import (
    EventEntry,
    EventRepository,
)
from ..exceptions import (
    AggregateNotFoundException,
    DeletedAggregateException,
)
from ..queries import (
    _Condition,
    _Ordering,
)
from ..transactions import (
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)
from .abc import (
    SnapshotRepository,
)

if TYPE_CHECKING:
    from ..models import (
        Aggregate,
    )


class InMemorySnapshotRepository(SnapshotRepository):
    """InMemory Snapshot class.

    The snapshot provides a direct accessor to the aggregate instances stored as events by the event repository class.
    """

    @inject
    def __init__(
        self,
        *args,
        event_repository: EventRepository = Provide["event_repository"],
        transaction_repository: TransactionRepository = Provide["transaction_repository"],
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if event_repository is None or isinstance(event_repository, Provide):
            raise NotProvidedException("An event repository instance is required.")

        if transaction_repository is None or isinstance(transaction_repository, Provide):
            raise NotProvidedException("A transaction repository instance is required.")

        self._event_repository = event_repository
        self._transaction_repository = transaction_repository

    async def _find(
        self,
        aggregate_name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[Aggregate]:
        uuids = {v.aggregate_uuid async for v in self._event_repository.select(aggregate_name=aggregate_name)}

        aggregates = list()
        for uuid in uuids:
            try:
                aggregate = await self.get(aggregate_name, uuid, **kwargs)
            except DeletedAggregateException:
                continue

            if condition.evaluate(aggregate):
                aggregates.append(aggregate)

        if ordering is not None:
            aggregates.sort(key=attrgetter(ordering.by), reverse=ordering.reverse)

        if limit is not None:
            aggregates = aggregates[:limit]

        for aggregate in aggregates:
            yield aggregate

    # noinspection PyMethodOverriding
    async def _get(
        self, aggregate_name: str, uuid: UUID, transaction: Optional[TransactionEntry] = None, **kwargs
    ) -> Aggregate:
        transaction_uuids = await self._get_transaction_uuids(transaction)
        entries = await self._get_event_entries(aggregate_name, uuid, transaction_uuids)

        if not len(entries):
            raise AggregateNotFoundException(f"Not found any entries for the {uuid!r} id.")

        if entries[-1].action.is_delete:
            raise DeletedAggregateException(f"The {uuid!r} id points to an already deleted aggregate.")

        return self._build_aggregate(entries, **kwargs)

    async def _get_transaction_uuids(self, transaction: Optional[TransactionEntry]) -> tuple[UUID, ...]:
        if transaction is None:
            transaction_uuids = (NULL_UUID,)
        else:
            transaction_uuids = await transaction.uuids

        while len(transaction_uuids) > 1:
            transaction = await self._transaction_repository.get(uuid=transaction_uuids[-1])
            if transaction.status != TransactionStatus.REJECTED:
                break
            transaction_uuids = tuple(transaction_uuids[:-1])

        return transaction_uuids

    async def _get_event_entries(
        self, aggregate_name: str, uuid: UUID, transaction_uuids: tuple[UUID, ...]
    ) -> list[EventEntry]:
        entries = [
            v
            async for v in self._event_repository.select(aggregate_name=aggregate_name, aggregate_uuid=uuid)
            if v.transaction_uuid in transaction_uuids
        ]

        entries.sort(key=lambda e: (e.version, transaction_uuids.index(e.transaction_uuid)))

        if len({e.transaction_uuid for e in entries}) > 1:
            new = [entries.pop()]
            for e in reversed(entries):
                if e.version < new[-1].version:
                    new.append(e)
            entries = list(reversed(new))
        return entries

    @staticmethod
    def _build_aggregate(entries: list[EventEntry], **kwargs) -> Aggregate:
        cls = entries[0].aggregate_cls
        aggregate = cls.from_diff(entries[0].aggregate_diff, **kwargs)
        for entry in entries[1:]:
            aggregate.apply_diff(entry.aggregate_diff)
        return aggregate

    async def _synchronize(self, **kwargs) -> None:
        pass
