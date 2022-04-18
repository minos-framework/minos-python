from __future__ import (
    annotations,
)

from contextlib import (
    suppress,
)
from typing import (
    Type,
)
from uuid import (
    UUID,
)

from minos.common import (
    Config,
    MinosConfigException,
    MinosJsonBinaryProtocol,
    MinosStorage,
    MinosStorageLmdb,
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


class DatabaseSagaExecutionRepository(SagaExecutionRepository):
    """Saga Execution Storage class."""

    def __init__(
        self,
        storage_cls: Type[MinosStorage] = MinosStorageLmdb,
        protocol=MinosJsonBinaryProtocol,
        db_name: str = "LocalState",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.db_name = db_name
        self._storage = storage_cls.build(protocol=protocol, **kwargs)

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> SagaExecutionRepository:
        with suppress(MinosConfigException):
            kwargs |= config.get_database_by_name("saga")
        return cls(**kwargs)

    async def _store(self, execution: SagaExecution) -> None:
        key = str(execution.uuid)
        value = execution.raw
        self._storage.update(table=self.db_name, key=key, value=value)

    async def _delete(self, uuid: UUID) -> None:
        self._storage.delete(table=self.db_name, key=str(uuid))

    async def _load(self, uuid: UUID) -> SagaExecution:
        value = self._storage.get(table=self.db_name, key=str(uuid))
        if value is None:
            raise SagaExecutionNotFoundException(f"The execution identified by {uuid} was not found.")
        execution = SagaExecution.from_raw(value)
        return execution
