from __future__ import (
    annotations,
)

from operator import (
    attrgetter,
)
from typing import (
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Union,
    get_args,
)
from uuid import (
    UUID,
    uuid4,
)

from minos.common import (
    DeclarativeModel,
    Model,
)

from .collections import (
    IncrementalSet,
    IncrementalSetDiff,
)


class Entity(DeclarativeModel):
    """Entity class ."""

    uuid: UUID

    def __init__(self, *args, uuid: Optional[UUID] = None, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)


T = TypeVar("T", bound=Model)


class EntitySet(IncrementalSet[T]):
    """Entity set class."""

    data: dict[str, T]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        if data is None:
            data = dict()
        elif not isinstance(data, dict):
            data = {str(entity.uuid): entity for entity in data}
        DeclarativeModel.__init__(self, data, *args, **kwargs)

    def add(self, entity: T) -> None:
        """Add an entity.

        :param entity: The entity to be added.
        :return: This method does not return anything.
        """
        self.data[str(entity.uuid)] = entity

    def discard(self, entity: T) -> None:
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

    def __iter__(self) -> Iterator[T]:
        yield from self.data.values()

    def __eq__(self, other):
        if isinstance(other, EntitySet):
            return super().__eq__(other)
        if isinstance(other, dict):
            return self.data == other
        return set(self) == other

    def diff(self, another: EntitySet[T]) -> IncrementalSetDiff:
        """Compute the difference between self and another entity set.

        :param another: Another entity set instance.
        :return: The difference between both entity sets.
        """
        return IncrementalSetDiff.from_difference(self, another, get_fn=attrgetter("uuid"))

    @property
    def data_cls(self) -> Optional[type]:
        """Get data class if available.

        :return: A model type.
        """
        args = get_args(self.type_hints["data"])
        return args[1]
