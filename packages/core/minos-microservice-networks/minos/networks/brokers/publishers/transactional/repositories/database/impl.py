from typing import (
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseMixin,
)

from ...entries import (
    BrokerPublisherTransactionEntry,
)
from ..abc import (
    BrokerPublisherTransactionRepository,
)
from .factories import (
    BrokerPublisherTransactionDatabaseOperationFactory,
)


class DatabaseBrokerPublisherTransactionRepository(
    BrokerPublisherTransactionRepository,
    DatabaseMixin[BrokerPublisherTransactionDatabaseOperationFactory],
):
    """Database Broker Publisher Transaction Repository class."""

    def __init__(self, *args, database_key: Optional[tuple[str]] = None, **kwargs):
        if database_key is None:
            database_key = ("broker",)
        super().__init__(*args, database_key=database_key, **kwargs)

    async def _setup(self) -> None:
        await super()._setup()
        await self._create_table()

    async def _create_table(self) -> None:
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _select(
        self, transaction_uuid: Optional[UUID], **kwargs
    ) -> AsyncIterator[BrokerPublisherTransactionEntry]:
        operation = self.database_operation_factory.build_query(transaction_uuid)
        async for raw in self.execute_on_database_and_fetch_all(operation):
            yield BrokerPublisherTransactionEntry(*raw)

    async def _submit(self, entry: BrokerPublisherTransactionEntry) -> None:
        operation = self.database_operation_factory.build_submit(**entry.as_raw())
        await self.execute_on_database(operation)

    async def _delete_batch(self, transaction_uuid: UUID) -> None:
        operation = self.database_operation_factory.build_delete_batch(transaction_uuid)
        await self.execute_on_database(operation)
