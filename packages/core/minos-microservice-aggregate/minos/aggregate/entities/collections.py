from __future__ import (
    annotations,
)

from operator import (
    attrgetter,
)
from typing import (
    Any,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    Union,
    get_args,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    DataDecoder,
    DataEncoder,
    DeclarativeModel,
    Model,
    ModelType,
    SchemaEncoder,
)

from ..collections import (
    IncrementalSet,
    IncrementalSetDiff,
)

T = TypeVar("T", bound=Model)


class EntitySet(IncrementalSet[T]):
    """Entity set class."""

    data: dict[str, T]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        DeclarativeModel.__init__(self, dict(), *args, **kwargs)

        if isinstance(data, dict):
            iterable = data.values()
        elif data is not None:
            iterable = data
        else:
            iterable = tuple()

        for entity in iterable:
            self.add(entity)

    def add(self, entity: T) -> None:
        """Add an entity.

        :param entity: The entity to be added.
        :return: This method does not return anything.
        """
        if entity.uuid == NULL_UUID:
            raise ValueError(f"The given entity must have a non-null uuid. Obtained entity: {entity!r}")
        self.data[str(entity.uuid)] = entity

    def discard(self, uuid: Union[T, UUID]) -> None:
        """Discard an entity.

        :param uuid: The entity to be discarded.
        :return: This method does not return anything.
        """
        if not isinstance(uuid, UUID):
            uuid = uuid.uuid
        self.data.pop(str(uuid), None)

    def get(self, uuid: UUID) -> T:
        """Get an entity by identifier.

        :param uuid: The identifier of the entity.
        :return: A entity instance.
        """
        return self.data[str(uuid)]

    def __contains__(self, uuid: Union[T, UUID]) -> bool:
        if not isinstance(uuid, UUID):
            if not hasattr(uuid, "uuid"):
                return False
            uuid = uuid.uuid
        return str(uuid) in self.data

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

    # noinspection PyMethodParameters
    @classmethod
    def encode_schema(cls, encoder: SchemaEncoder, target: Any, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :return: The encoded schema of the instance.
        """
        type_ = get_args(target.type_hints["data"])[-1]
        schema = encoder.build(list[type_], **kwargs)
        return schema | {"logicalType": cls.classname}

    @staticmethod
    def encode_data(encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :return: The encoded data of the instance.
        """
        target = list(target["data"].values())
        return encoder.build(target, **kwargs)

    @classmethod
    def decode_data(cls, decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> IncrementalSet:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :return: A decoded instance.
        """
        data_cls = get_args(type_.type_hints["data"])[1]
        target = (decoder.build(v, data_cls, **kwargs) for v in target)
        target = {str(v["uuid"]): v for v in target}
        decoded = decoder.build(target, type_.type_hints["data"], **kwargs)
        return cls(decoded, additional_type_hints=type_.type_hints)
