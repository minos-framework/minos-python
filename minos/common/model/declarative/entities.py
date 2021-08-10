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

from .abc import (
    DeclarativeModel,
)

T = TypeVar("T")


class Entity(DeclarativeModel):
    uuid: UUID

    def __init__(self, *args, uuid: Optional[UUID] = None, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)


class EntitySet(DeclarativeModel, MutableSet, Generic[T]):
    """Entity set class."""

    data: dict[str, Entity]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        if data is None:
            data = dict()
        elif not isinstance(data, dict):
            data = {str(entity.uuid): entity for entity in data}
        super().__init__(data, *args, **kwargs)

    def add(self, entity: Entity) -> NoReturn:
        """Add an entity.

        :param entity: The entity to be added.
        :return: This method does not return anything.
        """
        self.data[str(entity.uuid)] = entity

    def discard(self, entity: Entity) -> NoReturn:
        """Discard an entity.

        :param entity: The entity to be discarded.
        :return: This method does not return anything.
        """
        if not isinstance(entity, UUID):
            entity = entity.uuid
        del self.data[str(entity)]

    def get(self, uuid: UUID) -> T:
        """Get an entity by identifier.

        :param uuid: The identifier of the entity.
        :return: A entity instance.
        """
        return self.data[str(uuid)]

    def __contains__(self, entity: Union[Entity, UUID]) -> bool:
        if not isinstance(entity, UUID):
            if not hasattr(entity, "uuid"):
                return False
            entity = entity.uuid
        return entity in self.data

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[T]:
        yield from self.data.values()
