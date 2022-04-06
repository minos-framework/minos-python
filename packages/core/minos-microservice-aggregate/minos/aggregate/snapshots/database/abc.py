from __future__ import (
    annotations,
)

from typing import (
    Type,
    TypeVar, Optional,
)

from minos.common import (
    Config,
    DatabaseMixin,
)
from .factories import (
    SnapshotRepositoryOperationFactory,
    AiopgSnapshotRepositoryOperationFactory
)


class PostgreSqlSnapshotSetup(DatabaseMixin):
    """Minos Snapshot Setup Class"""

    def __init__(self, *args, operation_factory: Optional[SnapshotRepositoryOperationFactory] = None, **kwargs):
        super().__init__(*args, **kwargs)
        if operation_factory is None:
            operation_factory = AiopgSnapshotRepositoryOperationFactory()

        self.operation_factory = operation_factory

    @classmethod
    def _from_config(cls: Type[T], config: Config, **kwargs) -> T:
        return cls(database_key=None, **kwargs)

    async def _setup(self) -> None:
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)


T = TypeVar("T", bound=PostgreSqlSnapshotSetup)
