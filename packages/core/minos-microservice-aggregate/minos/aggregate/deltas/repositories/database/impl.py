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
    DeltaRepositoryConflictException,
)
from ...entries import (
    DeltaEntry,
)
from ..abc import (
    DeltaRepository,
)
from .factories import (
    DeltaDatabaseOperationFactory,
)


class DatabaseDeltaRepository(DatabaseMixin[DeltaDatabaseOperationFactory], DeltaRepository):
    """Database-based implementation of the delta repository class."""

    def __init__(self, *args, database_key: Optional[tuple[str]] = None, **kwargs):
        if database_key is None:
            database_key = ("aggregate", "delta")
        super().__init__(*args, database_key=database_key, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _submit(self, entry: DeltaEntry, **kwargs) -> DeltaEntry:
        operation = await self._build_submit_operation(entry)

        try:
            response = await self.execute_on_database_and_fetch_one(operation)
        except IntegrityException:
            raise DeltaRepositoryConflictException(
                f"{entry!r} could not be submitted due to a key (uuid, version, transaction) collision",
                await self.offset,
            )

        entry.id, entry.uuid, entry.version, entry.created_at = response
        return entry

    async def _build_submit_operation(self, entry: DeltaEntry) -> DatabaseOperation:
        lock = None
        if entry.uuid != NULL_UUID:
            lock = entry.uuid.int & (1 << 32) - 1

        if entry.transaction_uuid != NULL_UUID:
            transaction = await self.transaction_repository.get(uuid=entry.transaction_uuid)
            transaction_uuids = await transaction.uuids
        else:
            transaction_uuids = (NULL_UUID,)

        return self.database_operation_factory.build_submit(
            transaction_uuids=transaction_uuids, **entry.as_raw(), lock=lock
        )

    async def _select(self, streaming_mode: Optional[bool] = None, **kwargs) -> AsyncIterator[DeltaEntry]:
        operation = self.database_operation_factory.build_query(**kwargs)
        async for row in self.execute_on_database_and_fetch_all(operation, streaming_mode=streaming_mode):
            yield DeltaEntry(*row)

    @property
    async def _offset(self) -> int:
        operation = self.database_operation_factory.build_query_offset()
        row = await self.execute_on_database_and_fetch_one(operation)
        return row[0] or 0
