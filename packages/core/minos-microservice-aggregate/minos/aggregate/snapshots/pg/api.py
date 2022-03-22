from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Awaitable,
)

from minos.common import (
    Config,
)

from ..abc import (
    SnapshotRepository,
)
from .readers import (
    PostgreSqlSnapshotReader,
)
from .writers import (
    PostgreSqlSnapshotWriter,
)

if TYPE_CHECKING:
    from ...entities import (
        RootEntity,
    )


class PostgreSqlSnapshotRepository(SnapshotRepository):
    """PostgreSQL Snapshot class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
    """

    reader: PostgreSqlSnapshotReader
    writer: PostgreSqlSnapshotWriter

    def __init__(self, *args, reader: PostgreSqlSnapshotReader, writer: PostgreSqlSnapshotWriter, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = reader
        self.writer = writer

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> PostgreSqlSnapshotRepository:
        if "reader" not in kwargs:
            kwargs["reader"] = PostgreSqlSnapshotReader.from_config(config, **kwargs)

        if "writer" not in kwargs:
            kwargs["writer"] = PostgreSqlSnapshotWriter.from_config(config, **kwargs)

        return cls(**config.get_database_by_name("snapshot"), **kwargs)

    async def _setup(self) -> None:
        await self.writer.setup()
        await self.reader.setup()
        await super()._setup()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.reader.destroy()
        await self.writer.destroy()

    def _get(self, *args, **kwargs) -> Awaitable[RootEntity]:
        return self.reader.get(*args, **kwargs)

    def _find(self, *args, **kwargs) -> AsyncIterator[RootEntity]:
        return self.reader.find(*args, **kwargs)

    def _synchronize(self, *args, **kwargs) -> Awaitable[None]:
        return self.writer.dispatch(**kwargs)
