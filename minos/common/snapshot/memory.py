"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
    AsyncIterator,
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
from .conditions import (
    ANDCondition,
    Condition,
    FALSECondition,
    ORCondition,
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
        self, aggregate_name: str, condition: Condition, _repository: MinosRepository, **kwargs,
    ) -> AsyncIterator[Aggregate]:
        uuids = {v.aggregate_uuid async for v in _repository.select(aggregate_name=aggregate_name)}

        for uuid in uuids:
            aggregate = await self.get(aggregate_name, uuid, _repository, **kwargs)
            if self._matches_condition(aggregate, condition):
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
