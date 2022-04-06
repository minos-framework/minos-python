from __future__ import (
    annotations,
)

import warnings
from typing import (
    AsyncIterator,
    Optional,
)

from minos.common import (
    Config,
    DatabaseMixin,
)

from ....exceptions import (
    TransactionRepositoryConflictException,
)
from ...entries import (
    TransactionEntry,
)
from ..abc import (
    TransactionRepository,
)
from .factories import (
    AiopgTransactionRepositoryOperationFactory,
    TransactionRepositoryOperationFactory,
)


class DatabaseTransactionRepository(DatabaseMixin, TransactionRepository):
    """PostgreSql Transaction Repository class."""

    def __init__(self, *args, operation_factory: Optional[TransactionRepositoryOperationFactory] = None, **kwargs):
        super().__init__(*args, **kwargs)
        if operation_factory is None:
            operation_factory = AiopgTransactionRepositoryOperationFactory()

        self.operation_factory = operation_factory

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> DatabaseTransactionRepository:
        return super()._from_config(config, database_key=None, **kwargs)

    async def _setup(self):
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)

    async def _submit(self, transaction: TransactionEntry) -> TransactionEntry:
        operation = self.operation_factory.build_submit_row(
            uuid=transaction.uuid,
            destination_uuid=transaction.destination_uuid,
            status=transaction.status,
            event_offset=transaction.event_offset,
        )

        try:
            updated_at = await self.submit_query_and_fetchone(operation)
        except StopAsyncIteration:
            raise TransactionRepositoryConflictException(
                f"{transaction!r} status is invalid respect to the previous one."
            )
        transaction.updated_at = updated_at
        return transaction

    async def _select(self, **kwargs) -> AsyncIterator[TransactionEntry]:
        operation = self.operation_factory.build_select_rows(**kwargs)
        async for row in self.submit_query_and_iter(operation, **kwargs):
            yield TransactionEntry(*row, transaction_repository=self)


class PostgreSqlTransactionRepository(DatabaseTransactionRepository):
    """TODO"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            f"{PostgreSqlTransactionRepository!r} has been deprecated. Use {DatabaseTransactionRepository} instead.",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)
