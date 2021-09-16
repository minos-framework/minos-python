from __future__ import (
    annotations,
)

from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
    Optional,
)
from uuid import (
    UUID,
)

from ..exceptions import (
    MinosSnapshotAggregateNotFoundException,
    MinosSnapshotDeletedAggregateException,
)
from ..queries import (
    _Condition,
    _Ordering,
)
from ..repository import (
    MinosRepository,
)
from .abc import (
    MinosSnapshot,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class InMemorySnapshot(MinosSnapshot):
    """InMemory Snapshot class.

    The snapshot provides a direct accessor to the aggregate instances stored as events by the event repository class.
    """

    # noinspection PyMethodOverriding
    async def find(
        self,
        aggregate_name: str,
        condition: _Condition,
        ordering: Optional[_Ordering] = None,
        limit: Optional[int] = None,
        *,
        _repository: MinosRepository,
        **kwargs,
    ) -> AsyncIterator[Aggregate]:
        """Find a collection of ``Aggregate`` instances based on a ``Condition``.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param condition: The condition that must be satisfied by the ``Aggregate`` instances.
        :param ordering: Optional argument to return the instance with specific ordering strategy. The default behaviour
            is to retrieve them without any order pattern.
        :param limit: Optional argument to return only a subset of instances. The default behaviour is to return all the
            instances that meet the given condition.
        :param _repository: The event's ``Repository`` instance that provides access to the aggregate events needed to
            reconstruct the target aggregates.
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        uuids = {v.aggregate_uuid async for v in _repository.select(aggregate_name=aggregate_name)}

        aggregates = list()
        for uuid in uuids:
            aggregate = await self.get(aggregate_name, uuid, _repository=_repository, **kwargs)
            if condition.evaluate(aggregate):
                aggregates.append(aggregate)

        if ordering is not None:
            aggregates.sort(key=attrgetter(ordering.by), reverse=ordering.reverse)

        if limit is not None:
            aggregates = aggregates[:limit]

        for aggregate in aggregates:
            yield aggregate

    # noinspection PyMethodOverriding
    async def get(self, aggregate_name: str, uuid: UUID, *, _repository: MinosRepository, **kwargs) -> Aggregate:
        """Get an aggregate instance from its identifier.

        :param aggregate_name: Class name of the ``Aggregate``.
        :param uuid: Identifier of the ``Aggregate``.
        :param _repository: The event's ``Repository`` instance that provides access to the aggregate events needed to
            reconstruct the target aggregates.
        :param kwargs: Additional named arguments.
        :return: The ``Aggregate`` instance.
        """
        entries = [v async for v in _repository.select(aggregate_name=aggregate_name, aggregate_uuid=uuid)]
        if not len(entries):
            raise MinosSnapshotAggregateNotFoundException(f"Not found any entries for the {uuid!r} id.")

        entries.sort(key=attrgetter("version"))

        if entries[-1].action.is_delete:
            raise MinosSnapshotDeletedAggregateException(f"The {uuid!r} id points to an already deleted aggregate.")

        cls = entries[0].aggregate_cls
        instance: Aggregate = cls.from_diff(entries[0].aggregate_diff, _repository=_repository, **kwargs)
        for entry in entries[1:]:
            instance.apply_diff(entry.aggregate_diff)

        return instance

    # noinspection PyMethodOverriding
    async def synchronize(self, **kwargs) -> None:
        """Synchronize the snapshot to the latest available version.

        :param kwargs: Additional named arguments.
        :return: This method does not return anything.
        """
