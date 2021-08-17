"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from collections.abc import (
    MutableSet,
)
from typing import (
    Generic,
    Iterable,
    Iterator,
    NoReturn,
    Optional,
    TypeVar,
    Union,
)
from uuid import (
    UUID,
    uuid4,
)

from ..abc import (
    Model,
)
from ..actions import (
    Action,
)
from ..types import (
    ModelType,
)
from .abc import (
    DeclarativeModel,
)


class Entity(DeclarativeModel):
    """Entity class ."""

    uuid: UUID

    def __init__(self, *args, uuid: Optional[UUID] = None, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)


T = TypeVar("T", bound=Model)


class EntitySet(DeclarativeModel, MutableSet, Generic[T]):
    """Entity set class."""

    data: dict[str, T]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        if data is None:
            data = dict()
        elif not isinstance(data, dict):
            data = {str(entity.uuid): entity for entity in data}
        super().__init__(data, *args, **kwargs)

    def add(self, entity: T) -> NoReturn:
        """Add an entity.

        :param entity: The entity to be added.
        :return: This method does not return anything.
        """
        self.data[str(entity.uuid)] = entity

    def discard(self, entity: T) -> NoReturn:
        """Discard an entity.

        :param entity: The entity to be discarded.
        :return: This method does not return anything.
        """
        if not isinstance(entity, UUID):
            entity = entity.uuid
        self.data.pop(str(entity), None)

    def get(self, uuid: UUID) -> T:
        """Get an entity by identifier.

        :param uuid: The identifier of the entity.
        :return: A entity instance.
        """
        return self.data[str(uuid)]

    def __contains__(self, entity: Union[T, UUID]) -> bool:
        if not isinstance(entity, UUID):
            if not hasattr(entity, "uuid"):
                return False
            entity = entity.uuid
        return str(entity) in self.data

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[T]:
        yield from self.data.values()

    def __eq__(self, other):
        if isinstance(other, EntitySet):
            return super().__eq__(other)
        if isinstance(other, dict):
            return self.data == other
        return set(self) == other

    def diff(self, another: EntitySet[T]) -> EntitySetDiff:
        """Compute the difference between self and another entity set.

        :param another: Another entity set instance.
        :return: The difference between both entity sets.
        """
        return EntitySetDiff.from_difference(self, another)


EntitySetDiffEntry = ModelType.build("EntitySetDiffEntry", {"action": Action, "entity": Entity})


class EntitySetDiff(DeclarativeModel):
    """Entity Set Diff class."""

    diffs: list[EntitySetDiffEntry]

    @classmethod
    def from_difference(cls, new: EntitySet[T], old: EntitySet[T]) -> EntitySetDiff:
        """Build a new instance from two entity sets.

        :param new: The new entity set.
        :param old: The old entity set.
        :return: The difference between new and old.
        """
        differences = cls._diff(new, old)
        return cls(differences)

    @staticmethod
    def _diff(new: EntitySet[T], old: EntitySet[T]) -> list[EntitySetDiffEntry]:
        result = list()
        for entity in new - old:
            entry = EntitySetDiffEntry(Action.CREATE, entity)
            result.append(entry)

        for entity in old - new:
            entry = EntitySetDiffEntry(Action.DELETE, entity)
            result.append(entry)

        for entity in old & new:
            if entity == old.get(entity.uuid):
                continue
            entry = EntitySetDiffEntry(Action.UPDATE, entity)
            result.append(entry)

        return result
