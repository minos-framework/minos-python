from __future__ import (
    annotations,
)

from collections.abc import (
    MutableSet,
)
from typing import (
    Any,
    Callable,
    Generic,
    Iterable,
    Iterator,
    Optional,
    TypeVar,
    get_args,
)

from minos.common import (
    DataDecoder,
    DataEncoder,
    DeclarativeModel,
    ModelType,
    SchemaDecoder,
    SchemaEncoder,
)

from .actions import (
    Action,
)

T = TypeVar("T")


class IncrementalSet(DeclarativeModel, MutableSet, Generic[T]):
    """Incremental Set class."""

    data: set[T]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        if data is None:
            data = set()
        elif not isinstance(data, set):
            data = {value_obj for value_obj in data}
        super().__init__(data, *args, **kwargs)

    def add(self, value_object: T) -> None:
        """Add a value object.
        :param value_object: The value object to be added.
        :return: This method does not return anything.
        """
        self.data.add(value_object)

    def discard(self, value_object: T) -> None:
        """Remove a value object.
        :param value_object: The value object to be added.
        :return: This method does not return anything.
        """
        self.data.discard(value_object)

    def __contains__(self, value_object: T) -> bool:
        return value_object in self.data

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[T]:
        yield from self.data

    def __eq__(self, other: T) -> bool:
        if isinstance(other, IncrementalSet):
            return super().__eq__(other)
        return set(self) == other

    def diff(self, another: IncrementalSet[T]) -> IncrementalSetDiff:
        """Compute the difference between self and another entity set.
        :param another: Another entity set instance.
        :return: The difference between both entity sets.
        """
        return IncrementalSetDiff.from_difference(self, another)

    @property
    def data_cls(self) -> Optional[type]:
        """Get data class if available.

        :return: A model type.
        """
        args = get_args(self.type_hints["data"])
        return args[0]

    # noinspection PyMethodParameters
    @classmethod
    def encode_schema(cls, encoder: SchemaEncoder, target: Any, **kwargs) -> Any:
        """Encode schema with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded schema.
        :return: The encoded schema of the instance.
        """
        schema = encoder.build(target.type_hints["data"], **kwargs)
        return schema | {"logicalType": cls.classname}

    @classmethod
    def decode_schema(cls, decoder: SchemaDecoder, target: Any, **kwargs) -> ModelType:
        """Decode schema with the given encoder.

        :param decoder: The decoder instance.
        :param target: The schema to be decoded.
        :return: The decoded schema as a type.
        """
        decoded = decoder.build(target, **kwargs)
        return ModelType.from_model(cls[get_args(decoded)[-1]])

    @staticmethod
    def encode_data(encoder: DataEncoder, target: Any, **kwargs) -> Any:
        """Encode data with the given encoder.

        :param encoder: The encoder instance.
        :param target: An optional pre-encoded data.
        :return: The encoded data of the instance.
        """
        return encoder.build(target["data"], **kwargs)

    @classmethod
    def decode_data(cls, decoder: DataDecoder, target: Any, type_: ModelType, **kwargs) -> IncrementalSet:
        """Decode data with the given decoder.

        :param decoder: The decoder instance.
        :param target: The data to be decoded.
        :param type_: The data type.
        :return: A decoded instance.
        """
        decoded = decoder.build(target, type_.type_hints["data"], **kwargs)
        return cls(decoded, additional_type_hints=type_.type_hints)


IncrementalSetDiffEntry = ModelType.build("SetDiffEntry", {"action": Action, "entity": Any})


class IncrementalSetDiff(DeclarativeModel):
    """Incremental Set Diff class."""

    diffs: list[IncrementalSetDiffEntry]

    @classmethod
    def from_difference(
        cls, new: IncrementalSet[T], old: IncrementalSet[T], get_fn: Optional[Callable[[T], Any]] = None
    ) -> IncrementalSetDiff:
        """Build a new instance from two entity sets.
        :param new: The new entity set.
        :param old: The old entity set.
        :param get_fn: Optional function to get entries from the set by identifier.
        :return: The difference between new and old.
        """
        differences = cls._diff(new, old, get_fn)
        return cls(differences)

    @staticmethod
    def _diff(new: IncrementalSet[T], old: IncrementalSet[T], get_fn) -> list[IncrementalSetDiffEntry]:
        result = list()
        for value in new - old:
            entry = IncrementalSetDiffEntry(Action.CREATE, value)
            result.append(entry)

        for value in old - new:
            entry = IncrementalSetDiffEntry(Action.DELETE, value)
            result.append(entry)

        if get_fn is not None:
            for value in old & new:
                if value == old.get(value.uuid):
                    continue
                entry = IncrementalSetDiffEntry(Action.UPDATE, value)
                result.append(entry)

        return result
