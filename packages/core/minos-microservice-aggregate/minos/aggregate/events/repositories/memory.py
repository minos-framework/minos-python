from __future__ import (
    annotations,
)

from collections import (
    defaultdict,
)
from itertools import (
    count,
)
from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    NULL_UUID,
    current_datetime,
)

from ...exceptions import (
    EventRepositoryConflictException,
)
from ..entries import (
    EventEntry,
)
from .abc import (
    EventRepository,
)


class InMemoryEventRepository(EventRepository):
    """Memory-based implementation of the event repository class in ``minos``."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._storage = list()
        self._id_generator = count()
        self._next_versions = defaultdict(int)

    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        if entry.uuid == NULL_UUID:
            entry.uuid = uuid4()

        next_version = self._get_next_version_id(entry)
        if entry.version is None:
            entry.version = next_version
        if entry.version < next_version:
            raise EventRepositoryConflictException(
                f"{entry!r} could not be submitted due to a key (uuid, version, transaction) collision",
                await self.offset,
            )

        if entry.created_at is None:
            entry.created_at = current_datetime()

        entry.id = self._generate_next_id()
        self._storage.append(entry)
        return entry

    def _generate_next_id(self) -> int:
        return next(self._id_generator) + 1

    def _get_next_version_id(self, entry: EventEntry) -> int:
        key = (entry.name, entry.uuid, entry.transaction_uuid)
        self._next_versions[key] += 1
        return self._next_versions[key]

    async def _select(
        self,
        uuid: Optional[int] = None,
        name: Optional[str] = None,
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
        transaction_uuid_ne: Optional[UUID] = None,
        transaction_uuid_in: Optional[tuple[UUID, ...]] = None,
        *args,
        **kwargs,
    ) -> AsyncIterator[EventEntry]:

        # noinspection DuplicatedCode
        def _fn_filter(entry: EventEntry) -> bool:
            if uuid is not None and uuid != entry.uuid:
                return False
            if name is not None and name != entry.name:
                return False
            if version is not None and version != entry.version:
                return False
            if version_lt is not None and version_lt <= entry.version:
                return False
            if version_gt is not None and version_gt >= entry.version:
                return False
            if version_le is not None and version_le < entry.version:
                return False
            if version_ge is not None and version_ge > entry.version:
                return False
            if id is not None and id != entry.id:
                return False
            if id_lt is not None and id_lt <= entry.id:
                return False
            if id_gt is not None and id_gt >= entry.id:
                return False
            if id_le is not None and id_le < entry.id:
                return False
            if id_ge is not None and id_ge > entry.id:
                return False
            if transaction_uuid is not None and transaction_uuid != entry.transaction_uuid:
                return False
            if transaction_uuid_ne is not None and transaction_uuid_ne == entry.transaction_uuid:
                return False
            if transaction_uuid_in is not None and entry.transaction_uuid not in transaction_uuid_in:
                return False
            return True

        iterable = iter(self._storage)
        iterable = filter(_fn_filter, iterable)
        for item in iterable:
            yield item

    @property
    async def _offset(self) -> int:
        return len(self._storage)
