"""minos.common.snapshot.memory module."""

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
from ..repository import (
    MinosRepository,
)
from .abc import (
    MinosSnapshot,
)
from .queries import (
    ANDCondition,
    Condition,
    FALSECondition,
    ORCondition,
    Ordering,
    SimpleCondition,
    SimpleOperator,
    TRUECondition,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class InMemorySnapshot(MinosSnapshot):
    """In Memory Snapshot class."""

    # noinspection PyMethodOverriding
    async def find(
        self,
        aggregate_name: str,
        condition: Condition,
        ordering: Optional[Ordering] = None,
        limit: Optional[int] = None,
        _repository: MinosRepository = None,
        **kwargs,
    ) -> AsyncIterator[Aggregate]:
        """TODO

        :param aggregate_name: TODO
        :param condition: TODO
        :param ordering: TODO
        :param limit: TODO
        :param _repository: TODO
        :param kwargs: TODO
        :return: TODO
        """
        uuids = {v.aggregate_uuid async for v in _repository.select(aggregate_name=aggregate_name)}

        aggregates = list()
        for uuid in uuids:
            aggregate = await self.get(aggregate_name, uuid, _repository, **kwargs)
            if self._matches_condition(aggregate, condition):
                aggregates.append(aggregate)

        if ordering is not None:
            aggregates.sort(key=attrgetter(ordering.key), reverse=ordering.reverse)

        if limit is not None:
            aggregates = aggregates[:limit]

        for aggregate in aggregates:
            yield aggregate

    # noinspection PyMethodOverriding
    async def get(self, aggregate_name: str, uuid: UUID, _repository: MinosRepository, **kwargs) -> Aggregate:
        """Retrieve an asynchronous iterator that provides the requested ``Aggregate`` instances.

        :param aggregate_name: Class name of the ``Aggregate`` to be retrieved.
        :param uuid: Set of identifiers to be retrieved.
        :param _repository: TODO
        :param kwargs: Additional named arguments.
        :return: An asynchronous iterator that provides the requested ``Aggregate`` instances.
        """
        # noinspection PyTypeChecker
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

    def _matches_condition(self, aggregate: Aggregate, condition: Condition) -> bool:
        if isinstance(condition, TRUECondition):
            return True
        if isinstance(condition, FALSECondition):
            return False
        if isinstance(condition, ANDCondition):
            return all(self._matches_condition(aggregate, c) for c in condition.conditions)
        if isinstance(condition, ORCondition):
            return any(self._matches_condition(aggregate, c) for c in condition.conditions)
        if isinstance(condition, SimpleCondition):
            field = attrgetter(condition.field)(aggregate)
            value = condition.value
            if condition.operator == SimpleOperator.LOWER:
                return field < value
            if condition.operator == SimpleOperator.LOWER_EQUAL:
                return field <= value
            if condition.operator == SimpleOperator.GREATER:
                return field > value
            if condition.operator == SimpleOperator.GREATER_EQUAL:
                return field >= value
            if condition.operator == SimpleOperator.EQUAL:
                return field == value
            if condition.operator == SimpleOperator.NOT_EQUAL:
                return field != value
            if condition.operator == SimpleOperator.IN:
                return field in value

        raise Exception
