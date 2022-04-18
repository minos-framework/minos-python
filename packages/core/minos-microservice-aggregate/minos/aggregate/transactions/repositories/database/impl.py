from __future__ import (
    annotations,
)

from typing import (
    AsyncIterator,
    Optional,
)

from minos.common import (
    DatabaseMixin,
    ProgrammingException,
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
    TransactionDatabaseOperationFactory,
)


class DatabaseTransactionRepository(DatabaseMixin[TransactionDatabaseOperationFactory], TransactionRepository):
    """Database Transaction Repository class."""

    def __init__(self, *args, database_key: Optional[tuple[str]] = None, **kwargs):
        if database_key is None:
            database_key = ("aggregate", "transaction")
        super().__init__(*args, database_key=database_key, **kwargs)

    async def _setup(self):
        operation = self.database_operation_factory.build_create()
        await self.execute_on_database(operation)

    async def _submit(self, transaction: TransactionEntry) -> TransactionEntry:
        operation = self.database_operation_factory.build_submit(
            **transaction.as_raw(),
        )

        try:
            updated_at = await self.execute_on_database_and_fetch_one(operation)
        except ProgrammingException:
            raise TransactionRepositoryConflictException(
                f"{transaction!r} status is invalid respect to the previous one."
            )
        transaction.updated_at = updated_at
        return transaction

    async def _select(self, streaming_mode: Optional[bool] = None, **kwargs) -> AsyncIterator[TransactionEntry]:
        operation = self.database_operation_factory.build_query(**kwargs)
        async for row in self.execute_on_database_and_fetch_all(operation, streaming_mode=streaming_mode):
            yield TransactionEntry(*row, transaction_repository=self)
