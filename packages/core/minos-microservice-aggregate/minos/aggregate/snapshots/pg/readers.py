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

from minos.common import (
    NULL_UUID,
)

from ...exceptions import (
    NotFoundException,
)
from ...queries import (
    _Condition,
    _EqualCondition,
    _Ordering,
)
from ...transactions import (
    TransactionEntry,
)
from ..entries import (
    SnapshotEntry,
)
from .abc import (
    PostgreSqlSnapshotSetup,
)
from .queries import (
    PostgreSqlSnapshotQueryBuilder,
)

if TYPE_CHECKING:
    from ...entities import (
        RootEntity,
    )

logger = logging.getLogger(__name__)


class PostgreSqlSnapshotReader(PostgreSqlSnapshotSetup):
    """PostgreSQL Snapshot class.

    The snapshot provides a direct accessor to the ``RootEntity`` instances stored as events by the event repository
    class.
    """

    async def get(self, name: str, uuid: UUID, **kwargs) -> RootEntity:
        """Get a ``RootEntity`` instance from its identifier.

        :param name: Class name of the ``RootEntity``.
        :param uuid: Identifier of the ``RootEntity``.
        :param kwargs: Additional named arguments.
        :return: The ``RootEntity`` instance.
        """
        snapshot_entry = await self.get_entry(name, uuid, **kwargs)
        instance = snapshot_entry.build(**kwargs)
        return instance

    # noinspection PyUnusedLocal
    async def get_entry(self, name: str, uuid: UUID, **kwargs) -> SnapshotEntry:
        """Get a ``SnapshotEntry`` from its identifier.

        :param name: Class name of the ``RootEntity``.
        :param uuid: Identifier of the ``RootEntity``.
        :param kwargs: Additional named arguments.
        :return: The ``SnapshotEntry`` instance.
        """

        try:
            return await self.find_entries(
                name, _EqualCondition("uuid", uuid), **kwargs | {"exclude_deleted": False}
            ).__anext__()
        except StopAsyncIteration:
            raise NotFoundException(f"The instance could not be found: {uuid!s}")

    async def find(self, *args, **kwargs) -> AsyncIterator[RootEntity]:
        """Find a collection of ``RootEntity`` instances based on a ``Condition``.

        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``RootEntity`` instances.
        """
        async for snapshot_entry in self.find_entries(*args, **kwargs):
            yield snapshot_entry.build(**kwargs)

    async def find_entries(
        self,
        name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        streaming_mode: bool = False,
        transaction: Optional[TransactionEntry] = None,
        exclude_deleted: bool = True,
        **kwargs,
    ) -> AsyncIterator[SnapshotEntry]:
        """Find a collection of ``SnapshotEntry`` instances based on a ``Condition``.

        :param name: Class name of the ``RootEntity``.
        :param condition: The condition that must be satisfied by the ``RootEntity`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param streaming_mode: If ``True`` return the values in streaming directly from the database (keep an open
            database connection), otherwise preloads the full set of values on memory and then retrieves them.
        :param transaction: The transaction within the operation is performed. If not any value is provided, then the
            transaction is extracted from the context var. If not any transaction is being scoped then the query is
            performed to the global snapshot.
        :param exclude_deleted: If ``True``, deleted ``RootEntity`` entries are included, otherwise deleted
        ``RootEntity`` entries are filtered.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that containing the ``RootEntity`` instances.
        """
        if transaction is None:
            transaction_uuids = (NULL_UUID,)
        else:
            transaction_uuids = await transaction.uuids

        qb = PostgreSqlSnapshotQueryBuilder(name, condition, ordering, limit, transaction_uuids, exclude_deleted)
        query, parameters = qb.build()

        async with self.cursor() as cursor:
            # noinspection PyTypeChecker
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
