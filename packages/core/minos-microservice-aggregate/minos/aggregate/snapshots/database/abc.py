from __future__ import (
    annotations,
)

import warnings
from typing import (
    Optional,
    Type,
    TypeVar,
)

from minos.common import (
    Config,
    DatabaseMixin,
)

from .factories import (
    AiopgSnapshotDatabaseOperationFactory,
    SnapshotDatabaseOperationFactory,
)


class DatabaseSnapshotSetup(DatabaseMixin):
    """Minos Snapshot Setup Class"""

    def __init__(self, *args, operation_factory: Optional[SnapshotDatabaseOperationFactory] = None, **kwargs):
        super().__init__(*args, **kwargs)
        if operation_factory is None:
            operation_factory = AiopgSnapshotDatabaseOperationFactory()

        self.operation_factory = operation_factory

    @classmethod
    def _from_config(cls: Type[T], config: Config, **kwargs) -> T:
        return cls(database_key=None, **kwargs)

    async def _setup(self) -> None:
        operation = self.operation_factory.build_create_table()
        await self.submit_query(operation)


T = TypeVar("T", bound=DatabaseSnapshotSetup)


class PostgreSqlSnapshotSetup(DatabaseSnapshotSetup):
    """TODO"""

    def __init__(self, *args, **kwargs):
        warnings.warn(
            f"{PostgreSqlSnapshotSetup!r} has been deprecated. Use {DatabaseSnapshotSetup} instead.",
            DeprecationWarning,
        )
        super().__init__(*args, **kwargs)
