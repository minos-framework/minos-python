from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from functools import (
    cmp_to_key,
)
from operator import (
    attrgetter,
)
from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    Inject,
    NotProvidedException,
)
from minos.transactions import (
    TransactionEntry,
    TransactionRepository,
    TransactionStatus,
)

from ...deltas import (
    DeltaEntry,
    DeltaRepository,
)
from ...exceptions import (
    AlreadyDeletedException,
)
from ...queries import (
    _Condition,
    _Ordering,
)
from ..entries import (
    SnapshotEntry,
)
from .abc import (
    SnapshotRepository,
)


class InMemorySnapshotRepository(SnapshotRepository):
    """InMemory Snapshot class.

    The snapshot provides a direct accessor to the ``Entity`` instances stored as deltas by the delta repository
    class.
    """

    @Inject()
    def __init__(
        self,
        *args,
        delta_repository: DeltaRepository,
        transaction_repository: TransactionRepository,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)

        if delta_repository is None:
            raise NotProvidedException("An delta repository instance is required.")

        if transaction_repository is None:
            raise NotProvidedException("A transaction repository instance is required.")

        self._delta_repository = delta_repository
        self._transaction_repository = transaction_repository

    async def _find_entries(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering],
        limit: Optional[int],
        exclude_deleted: bool,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        uuids = {v.uuid async for v in self._delta_repository.select(name=name)}

        entries = list()
        for uuid in uuids:
            entry = await self._get(name, uuid, **kwargs)

            try:
                instance = entry.build()
                if condition.evaluate(instance):
                    entries.append(entry)
            except AlreadyDeletedException:
                # noinspection PyTypeChecker
                if not exclude_deleted and condition.evaluate(entry):
                    entries.append(entry)

        if ordering is not None:

            def _cmp(a: SnapshotEntry, b: SnapshotEntry) -> int:
                with suppress(AlreadyDeletedException):
                    with suppress(AlreadyDeletedException):
                        try:
                            aa = attrgetter(ordering.by)(a.build())
                        except AlreadyDeletedException:
                            aa = attrgetter(ordering.by)(a)
                    with suppress(AlreadyDeletedException):
                        try:
                            bb = attrgetter(ordering.by)(b.build())
                        except AlreadyDeletedException:
                            bb = attrgetter(ordering.by)(b)

                    if aa > bb:
                        return 1
                    elif aa < bb:
                        return -1

                return 0

            entries.sort(key=cmp_to_key(_cmp), reverse=ordering.reverse)

        if limit is not None:
            entries = entries[:limit]

        for entry in entries:
            yield entry

    # noinspection PyMethodOverriding
    async def _get(
        self, name: str, uuid: UUID, transaction: Optional[TransactionEntry] = None, **kwargs
    ) -> SnapshotEntry:
        transaction_uuids = await self._get_transaction_uuids(transaction)
        entries = await self._get_delta_entries(name, uuid, transaction_uuids)
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

    async def _get_delta_entries(self, name: str, uuid: UUID, transaction_uuids: tuple[UUID, ...]) -> list[DeltaEntry]:
        entries = [
            v
            async for v in self._delta_repository.select(name=name, uuid=uuid)
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
    def _build_instance(entries: list[DeltaEntry], **kwargs) -> SnapshotEntry:
        if entries[-1].action.is_delete:
            return SnapshotEntry.from_delta_entry(entries[-1])

        cls = entries[0].type_
        instance = cls.from_diff(entries[0].delta, **kwargs)
        for entry in entries[1:]:
            instance.apply_diff(entry.delta)

        snapshot = SnapshotEntry.from_entity(instance)

        return snapshot

    async def _synchronize(self, **kwargs) -> None:
        pass
