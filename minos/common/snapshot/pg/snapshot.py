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
# from ...exceptions import (
#     MinosSnapshotAggregateNotFoundException,
#     MinosSnapshotDeletedAggregateException,
# )
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
        if not await self.builder.is_synced(aggregate_name, **kwargs):
            await self.builder.dispatch(**kwargs)

        async for item in self._get(aggregate_name, uuids, **kwargs):
            yield item.build_aggregate(**kwargs)

    async def _get(
        self,
        aggregate_name: str,
        uuids: Optional[set[UUID]] = None,
        filters=None,
        streaming_mode: bool = False,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        # uniques = set(uuids)
        # if not len(uniques):
        #     return
        #
        # if len(uniques) != len(uuids):
        #     seen = set()
        #     duplicated = {x for x in uuids if x in seen or seen.add(x)}
        #     logger.warning(f"Duplicated identifiers will be ignored: {duplicated!r}")

        # parameters = (aggregate_name, tuple(uniques))

        parameters = [aggregate_name]
        if uuids is not None:
            parameters += [tuple(uuids)]
        if filters is not None:
            import json

            for filter_ in filters:
                parameters += [json.dumps(filter_[2])]

        async with self.cursor() as cursor:
            async with cursor.begin():

                query = self._query_builder(
                    _CHECK_MULTIPLE_ENTRIES_QUERY, aggregate_uuid=uuids is not None, filters=filters
                )
                await cursor.execute(query, parameters)
                total_count, not_null_count = await cursor.fetchone()

                query = self._query_builder(
                    _SELECT_MULTIPLE_ENTRIES_QUERY, aggregate_uuid=uuids is not None, filters=filters, ordering=True
                )
                await cursor.execute(query, parameters)

                # if total_count != len(uniques):
                #     # noinspection PyUnresolvedReferences
                #     found = {row[0] async for row in cursor}
                #     missing = uniques - found
                #     raise MinosSnapshotAggregateNotFoundException(f"Some aggregates could not be found: {missing!r}")
                #
                # if not_null_count != len(uniques):
                #     # noinspection PyUnresolvedReferences
                #     found = {row[0] async for row in cursor if row[3]}
                #     missing = uniques - found
                #     raise MinosSnapshotDeletedAggregateException(f"Some aggregates are already deleted: {missing!r}")

                if streaming_mode:
                    async for row in cursor:
                        # noinspection PyArgumentList
                        yield SnapshotEntry(*row)
                    return

                rows = await cursor.fetchall()

        for row in rows:
            yield SnapshotEntry(*row)

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[SnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self.submit_query_and_iter(_SELECT_ALL_ENTRIES_QUERY, **kwargs):
            yield SnapshotEntry(*row)

    def _query_builder(
        self,
        base: str,
        aggregate_uuid: bool = True,
        filters: Optional[list[tuple[str, str, Any]]] = None,
        ordering: bool = False,
    ):
        if filters is None:
            filters = list()
        base = str(base)

        if aggregate_uuid:
            base += " AND aggregate_uuid IN %s"

        for filter_ in filters:
            base += f"AND (data#>'{{{filter_[0].replace('.', ',')}}}') {filter_[1]} %s::jsonb"

        if ordering:
            base += " ORDER BY updated_at"

        return base


_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
ORDER BY updated_at
""".strip()

_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, schema, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s
""".strip()

_CHECK_MULTIPLE_ENTRIES_QUERY = """
SELECT COUNT(*) as total_count, COUNT(data) as not_null_count
FROM snapshot
WHERE aggregate_name = %s
""".strip()
