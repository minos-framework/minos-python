from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ...configuration import (
    MinosConfig,
)
from ...queries import (
    _Condition,
    _Ordering,
)
from ..abc import (
    MinosSnapshot,
)
from .readers import (
    PostgreSqlSnapshotReader,
)
from .writers import (
    PostgreSqlSnapshotWriter,
)

if TYPE_CHECKING:
    from ...model import (
        Aggregate,
    )


class PostgreSqlSnapshot(MinosSnapshot):
    """PostgreSQL Snapshot class.

   The snapshot provides a direct accessor to the aggregate instances stored as events by the event repository class.
    """

    reader: PostgreSqlSnapshotReader
    writer: PostgreSqlSnapshotWriter

    def __init__(self, *args, reader: PostgreSqlSnapshotReader, writer: PostgreSqlSnapshotWriter, **kwargs):
        super().__init__(*args, **kwargs)
        self.reader = reader
        self.writer = writer

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlSnapshot:
        if "reader" not in kwargs:
            kwargs["reader"] = PostgreSqlSnapshotReader.from_config(config=config, **kwargs)

        if "writer" not in kwargs:
            kwargs["writer"] = PostgreSqlSnapshotWriter.from_config(config=config, **kwargs)

        return cls(**config.snapshot._asdict(), **kwargs)

    async def _setup(self) -> None:
        await self.writer.setup()
        await self.reader.setup()
        await super()._setup()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.reader.destroy()
        await self.writer.destroy()

    async def get(self, aggregate_name: str, uuid: UUID, **kwargs) -> Aggregate:
        """Get an aggregate instance from its identifier.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param uuid: Identifier of the ``Aggregate``.
        :param kwargs: Additional named arguments.
        :return: The ``Aggregate`` instance.
        """
        await self.synchronize(**kwargs)

        return await self.reader.get(aggregate_name, uuid, **kwargs)

    async def find(
        self,
        aggregate_name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[Aggregate]:
        """Find a collection of ``Aggregate`` instances based on a ``Condition``.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param condition: The condition that must be satisfied by the ``Aggregate`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``Aggregate`` instances.
        """
        await self.synchronize(**kwargs)

        async for aggregate in self.reader.find(aggregate_name, condition, ordering, limit, **kwargs):
            yield aggregate

    async def synchronize(self, **kwargs) -> None:
        """Synchronize the snapshot to the latest available version.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
        await self.writer.dispatch(**kwargs)
