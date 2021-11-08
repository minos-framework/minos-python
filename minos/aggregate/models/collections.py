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
    DeclarativeModel,
    ModelType,
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
        """Add an value object.
        :param value_object: The value object to be added.
        :return: This method does not return anything.
        """
        self.data.add(value_object)

    def discard(self, value_object: T) -> None:
        """Remove an value object.
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
