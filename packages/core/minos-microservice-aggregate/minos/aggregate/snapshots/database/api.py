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
    DatabaseSnapshotReader,
)
from .writers import (
    DatabaseSnapshotWriter,
)

if TYPE_CHECKING:
    from ...entities import (
        RootEntity,
    )


class DatabaseSnapshotRepository(SnapshotRepository):
    """Database Snapshot Repository class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
    """

    reader: DatabaseSnapshotReader
    writer: DatabaseSnapshotWriter

    def __init__(self, *args, reader: DatabaseSnapshotReader, writer: DatabaseSnapshotWriter, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = reader
        self.writer = writer

    @classmethod
    def _from_config(cls, config: Config, **kwargs) -> DatabaseSnapshotRepository:
        if "reader" not in kwargs:
            kwargs["reader"] = DatabaseSnapshotReader.from_config(config, **kwargs)

        if "writer" not in kwargs:
            kwargs["writer"] = DatabaseSnapshotWriter.from_config(config, **kwargs)

        return cls(database_key=None, **kwargs)

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