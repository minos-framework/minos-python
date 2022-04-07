from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    Optional,
)

from minos.common import (
    NULL_UUID,
    Config,
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


class DatabaseEventRepository(DatabaseMixin, EventRepository):
    """PostgreSQL-based implementation of the event repository class in ``Minos``."""

    def __init__(
        self,
        *args,
        operation_factory: Optional[EventDatabaseOperationFactory] = None,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        if operation_factory is None:
            operation_factory = self.pool_instance_cls.get_factory(EventDatabaseOperationFactory)

        self.operation_factory = operation_factory

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> Optional[EventRepository]:
        return super()._from_config(config, database_key=None, **kwargs)

    async def _setup(self):
        """Setup miscellaneous repository thing.

        In the PostgreSQL case, configures the needed table to be used to store the data.

        :return: This method does not return anything.
        """
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)

    async def _submit(self, entry: EventEntry, **kwargs) -> EventEntry:
        operation = await self._build_submit_operation(entry)

        try:
            response = await self.submit_query_and_fetchone(operation)
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

        return self.operation_factory.build_submit_row(transaction_uuids=transaction_uuids, **entry.as_raw(), lock=lock)

    async def _select(self, streaming_mode: Optional[bool] = None, **kwargs) -> AsyncIterator[EventEntry]:
        operation = self.operation_factory.build_select_rows(**kwargs)
        async for row in self.submit_query_and_iter(operation, streaming_mode=streaming_mode):
            yield EventEntry(*row)

    @property
    async def _offset(self) -> int:
        operation = self.operation_factory.build_select_max_id()
        row = await self.submit_query_and_fetchone(operation)
        return row[0] or 0
