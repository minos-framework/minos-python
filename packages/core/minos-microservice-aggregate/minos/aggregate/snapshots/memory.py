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
    AlreadyDeletedException,
    NotFoundException,
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
    from ..entities import (
        RootEntity,
    )


class InMemorySnapshotRepository(SnapshotRepository):
    """InMemory Snapshot class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
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
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        **kwargs,
    ) -> AsyncIterator[RootEntity]:
        uuids = {v.uuid async for v in self._event_repository.select(name=name)}

        instances = list()
        for uuid in uuids:
            try:
                instance = await self.get(name, uuid, **kwargs)
            except AlreadyDeletedException:
                continue

            if condition.evaluate(instance):
                instances.append(instance)

        if ordering is not None:
            instances.sort(key=attrgetter(ordering.by), reverse=ordering.reverse)

        if limit is not None:
            instances = instances[:limit]

        for instance in instances:
            yield instance

    # noinspection PyMethodOverriding
    async def _get(self, name: str, uuid: UUID, transaction: Optional[TransactionEntry] = None, **kwargs) -> RootEntity:
        transaction_uuids = await self._get_transaction_uuids(transaction)
        entries = await self._get_event_entries(name, uuid, transaction_uuids)

        if not len(entries):
            raise NotFoundException(f"Not found any entries for the {uuid!r} id.")

        if entries[-1].action.is_delete:
            raise AlreadyDeletedException(f"The {uuid!r} identifier belongs to an already deleted instance.")

        return self._build_instance(entries, **kwargs)

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

    async def _get_event_entries(self, name: str, uuid: UUID, transaction_uuids: tuple[UUID, ...]) -> list[EventEntry]:
        entries = [
            v
            async for v in self._event_repository.select(name=name, uuid=uuid)
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
    def _build_instance(entries: list[EventEntry], **kwargs) -> RootEntity:
        cls = entries[0].type_
        instance = cls.from_diff(entries[0].event, **kwargs)
        for entry in entries[1:]:
            instance.apply_diff(entry.event)
        return instance

    async def _synchronize(self, **kwargs) -> None:
        pass
