from __future__ import (
    annotations,
)

from typing import (
    Optional,
)
from uuid import (
    UUID,
)

from minos.common import (
    DatabaseMixin,
    ProgrammingException,
)

from ....exceptions import (
    SagaExecutionNotFoundException,
)
from ...saga import (
    SagaExecution,
)
from ..abc import (
    SagaExecutionRepository,
)
from .factories import (
    SagaExecutionDatabaseOperationFactory,
)


class DatabaseSagaExecutionRepository(SagaExecutionRepository, DatabaseMixin[SagaExecutionDatabaseOperationFactory]):
    """Saga Execution Storage class."""

    def __init__(self, *args, database_key: Optional[tuple[str]] = None, **kwargs):
        if database_key is None:
            database_key = ("saga",)
        super().__init__(*args, database_key=database_key, **kwargs)

    async def _store(self, execution: SagaExecution) -> None:
        operation = self.database_operation_factory.build_store(**execution.raw)
        await self.execute_on_database(operation)

    async def _delete(self, uuid: UUID) -> None:
        operation = self.database_operation_factory.build_delete(uuid)
        await self.execute_on_database(operation)

    async def _load(self, uuid: UUID) -> SagaExecution:
        operation = self.database_operation_factory.build_load(uuid)

        try:
            value = await self.execute_on_database_and_fetch_one(operation)
        except ProgrammingException:
            raise SagaExecutionNotFoundException(f"The execution identified by {uuid} was not found.")
        execution = SagaExecution.from_raw(value)
        return execution
