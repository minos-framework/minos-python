"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    NoReturn,
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
    def _from_config(cls, *args, config: MinosConfig, **kwargs) -> PostgreSqlSnapshot:
        builder = PostgreSqlSnapshotBuilder.from_config(*args, config=config, **kwargs)
        return cls(*args, builder=builder, **config.snapshot._asdict(), **kwargs)

    async def _setup(self) -> NoReturn:
        await self.builder.setup()
        await super()._setup()

    async def _destroy(self) -> NoReturn:
        await super()._destroy()
        await self.builder.destroy()

    async def get(self, aggregate_name: str, uuids: set[UUID], **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuids: Set of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        # noinspection PyShadowingBuiltins
        if not await self.builder.are_synced(aggregate_name, uuids, **kwargs):
            await self.builder.dispatch(**kwargs)

        async for item in self._get(aggregate_name, uuids, **kwargs):
            yield item.build_aggregate(**kwargs)

    async def _get(
        self, aggregate_name: str, uuids: set[UUID], streaming_mode: bool = False, **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
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

                await cursor.execute(_SELECT_MULTIPLE_ENTRIES_QUERY, parameters)

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

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[SnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self.submit_query_and_iter(_SELECT_ALL_ENTRIES_QUERY, **kwargs):
            yield SnapshotEntry(*row)


_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, data, created_at, updated_at
FROM snapshot
ORDER BY updated_at;
""".strip()

_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_uuid, aggregate_name, version, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s AND aggregate_uuid IN %s
ORDER BY updated_at;
""".strip()

_CHECK_MULTIPLE_ENTRIES_QUERY = """
SELECT COUNT(*) as total_count, COUNT(data) as not_null_count
FROM snapshot
WHERE aggregate_name = %s AND aggregate_uuid IN %s;
""".strip()
