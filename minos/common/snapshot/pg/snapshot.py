"""minos.common.snapshot.pg.snapshot module."""

from __future__ import (
    annotations,
)

import logging
from typing import (
    TYPE_CHECKING,
    Any,
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
    MinosSnapshotDeletedAggregateException,
)
from ..abc import (
    MinosSnapshot,
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

    async def get(self, aggregate_name: str, uuids: Optional[set[UUID]] = None, **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuids: Set of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        async for snapshot_entry in self.get_entries(aggregate_name, uuids, **kwargs):
            yield snapshot_entry.build_aggregate(**kwargs)

    async def get_entries(
        self, aggregate_name: str, uuids: set[UUID], streaming_mode: bool = False, **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        if not await self.builder.is_synced(aggregate_name, **kwargs):
            await self.builder.dispatch(**kwargs)

        uniques = set(uuids)

        if not len(uniques):
            return

        if len(uniques) != len(uuids):
            seen = set()
            duplicated = {x for x in uuids if x in seen or seen.add(x)}
            logger.warning(f"Duplicated identifiers will be ignored: {duplicated!r}")

        parameters = (aggregate_name, tuple(uniques))

        async with self.cursor() as cursor:
            async with cursor.begin():
                await cursor.execute(_CHECK_MULTIPLE_ENTRIES_QUERY, parameters)
                total_count, not_null_count = await cursor.fetchone()

                await cursor.execute(_SELECT_ENTRIES_BY_UUID_QUERY, parameters)

                if total_count != len(uniques):
                    # noinspection PyUnresolvedReferences
                    found = {row[0] async for row in cursor}
                    missing = uniques - found
                    raise MinosSnapshotAggregateNotFoundException(f"Some aggregates could not be found: {missing!r}")

                if not_null_count != len(uniques):
                    # noinspection PyUnresolvedReferences
                    found = {row[0] async for row in cursor if row[3]}
                    missing = uniques - found
                    raise MinosSnapshotDeletedAggregateException(f"Some aggregates are already deleted: {missing!r}")

                if streaming_mode:
                    async for row in cursor:
                        # noinspection PyArgumentList
                        yield SnapshotEntry(*row)
                    return

                rows = await cursor.fetchall()

        for row in rows:
            yield SnapshotEntry(*row)

    async def find(self, aggregate_name: str, filters: Optional[list], **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param filters: TODO.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        async for snapshot_entry in self.find_entries(aggregate_name, filters, **kwargs):
            yield snapshot_entry.build_aggregate(**kwargs)

    async def find_entries(
        self,
        aggregate_name: str,
        filters,
        ordering: Optional = None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        """TODO

        :param aggregate_name: TODO
        :param filters: TODO
        :param ordering: TODO
        :param streaming_mode: TODO
        :param kwargs: TODO
        :return: TODO
        """
        if not await self.builder.is_synced(aggregate_name, **kwargs):
            await self.builder.dispatch(**kwargs)

        parameters = [aggregate_name]
        import json

        for filter_ in filters:
            parameters += [json.dumps(filter_[2])]

        async with self.cursor() as cursor:
            async with cursor.begin():
                query = self._query_builder(filters=filters, ordering=ordering)
                await cursor.execute(query, parameters)

                if streaming_mode:
                    async for row in cursor:
                        # noinspection PyArgumentList
                        yield SnapshotEntry(*row)
                    return

                rows = await cursor.fetchall()

        for row in rows:
            yield SnapshotEntry(*row)

    @staticmethod
    def _query_builder(filters: Optional[list[tuple[str, str, Any]]], ordering: bool = False) -> str:
        base = str(_SELECT_MULTIPLE_ENTRIES_QUERY)

        for filter_ in filters:
            base += f"AND (data#>'{{{filter_[0].replace('.', ',')}}}') {filter_[1]} %s::jsonb"

        if ordering:
            base += " ORDER BY updated_at"

        return base


_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s
""".strip()

_SELECT_ENTRIES_BY_UUID_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s AND aggregate_uuid IN %s
ORDER BY updated_at
""".strip()

_CHECK_MULTIPLE_ENTRIES_QUERY = """
SELECT COUNT(*) as total_count, COUNT(data) as not_null_count
FROM snapshot
WHERE aggregate_name = %s AND aggregate_uuid IN %s
""".strip()
