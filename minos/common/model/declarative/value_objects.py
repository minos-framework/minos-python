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
    Any,
    Generic,
    Iterable,
    Iterator,
    NoReturn,
    Optional,
    TypeVar,
)

from ...exceptions import (
    MinosImmutableClassException,
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


class ValueObject(DeclarativeModel):
    """Value Object class."""

    def __setattr__(self, key: str, value: Any):
        if key.startswith("_"):
            super().__setattr__(key, value)
        else:
            raise MinosImmutableClassException("modification of an immutable value object not allowed")


T = TypeVar("T", bound=Model)


class ValueObjectSet(DeclarativeModel, MutableSet, Generic[T]):
    """Value Object Set class."""

    data: dict[str, T]

    def __init__(self, data: Optional[Iterable[T]] = None, *args, **kwargs):
        if data is None:
            data = dict()
        elif not isinstance(data, dict):
            data = {str(hash(value_obj)): value_obj for value_obj in data}
        super().__init__(data, *args, **kwargs)

    def add(self, value_object: T) -> NoReturn:
        """Add an value object.
        :param value_object: The value object to be added.
        :return: This method does not return anything.
        """
        self.data[str(hash(value_object))] = value_object

    def discard(self, value_object: T) -> NoReturn:
        """Remove an value object.
        :param value_object: The value object to be added.
        :return: This method does not return anything.
        """
        self.data.pop(str(hash(value_object)), None)

    def __contains__(self, value_object: T) -> bool:
        if not isinstance(value_object, ValueObject):
            return False
        return str(hash(value_object)) in self.data

    def __len__(self) -> int:
        return len(self.data)

    def __iter__(self) -> Iterator[T]:
        yield from self.data.values()

    def __eq__(self, other: T) -> bool:
        if isinstance(other, ValueObjectSet):
            return super().__eq__(other)
        if isinstance(other, dict):
            return self.data == other
        return set(self) == other

    def diff(self, another: ValueObjectSet[T]) -> ValueObjectSetDiff:
        """Compute the difference between self and another entity set.
        :param another: Another entity set instance.
        :return: The difference between both entity sets.
        """
        return ValueObjectSetDiff.from_difference(self, another)


ValueObjectSetDiffEntry = ModelType.build("EntitySetDiffEntry", {"action": Action, "entity": ValueObject})


class ValueObjectSetDiff(DeclarativeModel):
    """Value Object Set Diff class."""

    diffs: list[ValueObjectSetDiffEntry]

    @classmethod
    def from_difference(cls, new: ValueObjectSet[T], old: ValueObjectSet[T]) -> ValueObjectSetDiff:
        """Build a new instance from two entity sets.
        :param new: The new entity set.
        :param old: The old entity set.
        :return: The diference between new and old.
        """
        differences = cls._diff(new, old)
        return cls(differences)

    @staticmethod
    def _diff(new: ValueObjectSet[T], old: ValueObjectSet[T]) -> list[ValueObjectSetDiffEntry]:
        result = list()
        for entity in new - old:
            entry = ValueObjectSetDiffEntry(Action.CREATE, entity)
            result.append(entry)

        for entity in old - new:
            entry = ValueObjectSetDiffEntry(Action.DELETE, entity)
            result.append(entry)

        return result
