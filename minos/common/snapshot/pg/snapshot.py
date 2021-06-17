"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    NoReturn,
)

from ...configuration import (
    MinosConfig,
)
from ...exceptions import (
    MinosRepositoryAggregateNotFoundException,
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

    async def get(self, aggregate_name: str, ids: list[int], **kwargs) -> AsyncIterator[Aggregate]:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param ids: List of identifiers to be retrieved.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        # noinspection PyShadowingBuiltins
        if not await self.builder.are_synced(aggregate_name, ids):
            await self.builder.dispatch()

        async for item in self._get(aggregate_name, ids):
            yield item.aggregate

    async def _get(self, aggregate_name: str, ids: list[int]) -> AsyncIterator[SnapshotEntry]:
        found = set()
        async for row in self.submit_query_and_iter(_SELECT_MULTIPLE_ENTRIES_QUERY, (aggregate_name, tuple(ids))):
            found.add(row[0])
            yield SnapshotEntry(*row)

        missing = set(ids) - found
        if missing:
            raise MinosRepositoryAggregateNotFoundException(f"Not found entries identified with {missing!r} ids.")

    # noinspection PyUnusedLocal
    async def select(self, *args, **kwargs) -> AsyncIterator[SnapshotEntry]:
        """Select a sequence of ``MinosSnapshotEntry`` objects.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A sequence of ``MinosSnapshotEntry`` objects.
        """
        async for row in self.submit_query_and_iter(_SELECT_ALL_ENTRIES_QUERY):
            yield SnapshotEntry(*row)


_SELECT_ALL_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, created_at, updated_at
FROM snapshot;
""".strip()

_SELECT_MULTIPLE_ENTRIES_QUERY = """
SELECT aggregate_id, aggregate_name, version, data, created_at, updated_at
FROM snapshot
WHERE aggregate_name = %s AND aggregate_id IN %s;
""".strip()
