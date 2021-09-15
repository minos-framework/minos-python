"""minos.common.snapshot.pg.snapshot module."""

from __future__ import (
    annotations,
)

import logging
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
from ...exceptions import (
    MinosSnapshotAggregateNotFoundException,
)
from ..abc import (
    MinosSnapshot,
)
from ..conditions import (
    Condition,
)
from ..entries import (
    SnapshotEntry,
)
from .abc import (
    PostgreSqlSnapshotSetup,
)
from .builders import (
    PostgreSqlSnapshotBuilder,
)
from .conditions import (
    PostgreSqlSnapshotQueryBuilder,
)

if TYPE_CHECKING:
    from ...model import (
        Aggregate,
    )

logger = logging.getLogger(__name__)


class PostgreSqlSnapshot(PostgreSqlSnapshotSetup, MinosSnapshot):
    """Minos Snapshot Reader class."""

    builder: PostgreSqlSnapshotBuilder

    def __init__(self, *args, builder: PostgreSqlSnapshotBuilder, **kwargs):
        super().__init__(*args, **kwargs)
        self.builder = builder

    @classmethod
    def _from_config(cls, config: MinosConfig, **kwargs) -> PostgreSqlSnapshot:
        builder = PostgreSqlSnapshotBuilder.from_config(config=config, **kwargs)
        return cls(builder=builder, **config.snapshot._asdict(), **kwargs)

    async def _setup(self) -> None:
        await self.builder.setup()
        await super()._setup()

    async def _destroy(self) -> None:
        await super()._destroy()
        await self.builder.destroy()

    async def get(self, aggregate_name: str, uuid: UUID, **kwargs) -> Aggregate:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuid: Set of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        snapshot_entry = await self.get_entry(aggregate_name, uuid, **kwargs)
        aggregate = snapshot_entry.build_aggregate(**kwargs)
        return aggregate

    async def get_entry(self, aggregate_name: str, uuid: UUID, **kwargs) -> SnapshotEntry:
        if not await self.builder.is_synced(aggregate_name, **kwargs):
            await self.builder.dispatch(**kwargs)

        parameters = {"aggregate_name": aggregate_name, "aggregate_uuid": uuid}

        async with self.cursor() as cursor:
            await cursor.execute(_SELECT_ENTRY_BY_UUID_QUERY, parameters)
            row = await cursor.fetchone()
            if row is None:
                raise MinosSnapshotAggregateNotFoundException(f"Some aggregates could not be found: {uuid!s}")

            return SnapshotEntry(*row)

    async def find(self, aggregate_name: str, condition: Condition, **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param condition: TODO.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        async for snapshot_entry in self.find_entries(aggregate_name, condition, **kwargs):
            yield snapshot_entry.build_aggregate(**kwargs)

    async def find_entries(
        self,
        aggregate_name: str,
        condition: Condition,
        ordering: Optional[str] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        """TODO

        :param aggregate_name: TODO
        :param condition: TODO
        :param ordering: TODO
        :param limit: TODO
        :param streaming_mode: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if not await self.builder.is_synced(aggregate_name, **kwargs):
            await self.builder.dispatch(**kwargs)

        query, parameters = PostgreSqlSnapshotQueryBuilder(aggregate_name, condition, ordering, limit).build()

        async with self.cursor() as cursor:
            await cursor.execute(query, parameters)
            if streaming_mode:
                async for row in cursor:
                    # noinspection PyArgumentList
                    yield SnapshotEntry(*row)
                return
            else:
                rows = await cursor.fetchall()
        for row in rows:
            yield SnapshotEntry(*row)


_SELECT_ENTRY_BY_UUID_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %(aggregate_name)s AND aggregate_uuid = %(aggregate_uuid)s
""".strip()
