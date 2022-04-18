from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    Optional,
)

from minos.common import (
    NULL_UUID,
    DatabaseMixin,
    DatabaseOperation,
    IntegrityException,
)

from ....exceptions import (
    EventRepositoryConflictException,
)
from ...entries import (
    EventEntry,
)
from ..abc import (
    EventRepository,
)
from .factories import (
    EventDatabaseOperationFactory,
)


class DatabaseEventRepository(DatabaseMixin[EventDatabaseOperationFactory], EventRepository):
    """Database-based implementation of the event repository class."""

    def __init__(self, *args, database_key: Optional[tuple[str]] = None, **kwargs):
        if database_key is None:
            database_key = ("aggregate", "event")
        super().__init__(*args, database_key=database_key, **kwargs)

    async def _setup(self):
        """Setup miscellaneous repository things.

        :return: This method does not return anything.
        """
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        operation = await self._build_submit_operation(entry)

        try:
            response = await self.execute_on_database_and_fetch_one(operation)
        except IntegrityException:
            raise EventRepositoryConflictException(
                f"{entry!r} could not be submitted due to a key (uuid, version, transaction) collision",
                await self.offset,
            )

        entry.id, entry.uuid, entry.version, entry.created_at = response
        return entry

    async def _build_submit_operation(self, entry: EventEntry) -> DatabaseOperation:
        lock = None
        if entry.uuid != NULL_UUID:
            lock = entry.uuid.int & (1 << 32) - 1

        if entry.transaction_uuid != NULL_UUID:
            transaction = await self._transaction_repository.get(uuid=entry.transaction_uuid)
            transaction_uuids = await transaction.uuids
        else:
            transaction_uuids = (NULL_UUID,)

        return self.database_operation_factory.build_submit(
            transaction_uuids=transaction_uuids, **entry.as_raw(), lock=lock
        )

    async def _select(self, streaming_mode: Optional[bool] = None, **kwargs) -> AsyncIterator[EventEntry]:
        operation = self.database_operation_factory.build_query(**kwargs)
        async for row in self.execute_on_database_and_fetch_all(operation, streaming_mode=streaming_mode):
            yield EventEntry(*row)

    @property
    async def _offset(self) -> int:
        operation = self.database_operation_factory.build_query_offset()
        row = await self.execute_on_database_and_fetch_one(operation)
        return row[0] or 0
