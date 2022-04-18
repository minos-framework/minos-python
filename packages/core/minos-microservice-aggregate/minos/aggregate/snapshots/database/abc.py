from __future__ import (
    annotations,
)

from typing import (
    Type,
    TypeVar,
)

from minos.common import (
    Config,
    DatabaseMixin,
)

from .factories import (
    SnapshotDatabaseOperationFactory,
)


class DatabaseSnapshotSetup(DatabaseMixin[SnapshotDatabaseOperationFactory]):
    """Minos Snapshot Setup Class"""

    @classmethod
    def _from_config(cls: Type[T], config: Config, **kwargs) -> T:
        return cls(database_key=None, **kwargs)

    async def _setup(self) -> None:
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)


T = TypeVar("T", bound=DatabaseSnapshotSetup)
