"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from collections import (
    defaultdict,
)
from itertools import (
    chain,
)
from typing import (
    Any,
    Generic,
    Iterable,
    Iterator,
    Type,
    TypeVar,
    Union,
    get_args,
)
from uuid import (
    uuid4,
)

from ..abc import (
    Model,
)
from ..actions import (
    Action,
)
from ..fields import (
    Field,
)
from ..types import (
    ModelType,
)
from .bucket import (
    BucketModel,
)

logger = logging.getLogger(__name__)

T = TypeVar("T")


class FieldDiff(Model, Generic[T]):
    """Field Diff class."""

    name: str
    value: T

    def __init__(self, name: str, type_: Type, value: Any):
        super().__init__([Field("name", str, name), Field("value", type_, value)])

    @classmethod
    def from_model_type(cls, model_type: ModelType, *args, **kwargs) -> FieldDiff:
        """Build a new instance from model type.

        :param model_type: The model type.
        :param args: Additional positional. arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``FieldDiff`` instance.
        """
        kwargs["type_"] = model_type.type_hints["value"]
        return cls(*args, **kwargs)


class IncrementalFieldDiff(FieldDiff, Generic[T]):
    """Incremental Field Diff class."""

    action: Action

    def __init__(self, name: str, type_: Type, value: Any, action: Action):
        Model.__init__(self, [Field("name", str, name), Field("value", type_, value), Field("action", Action, action)])


class FieldDiffContainer(BucketModel):
    """Field Diff Container class."""

    def __init__(
        self, diffs: Iterable[FieldDiff] = None, fields: Union[Iterable[Field], dict[str, Field]] = None, **kwargs
    ):
        if diffs is not None:
            fields = map(lambda v: Field(self.generate_random_str(), FieldDiff, v), diffs)

        super().__init__(fields, **kwargs)

        self._mapper = _build_mapper(self._fields)

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            try:
                return self.get_one(item)
            except Exception:
                raise exc

    def __eq__(self, other):
        return type(self) == type(other) and tuple(self.values()) == tuple(other.values())

    def __repr__(self) -> str:
        fields_repr = ", ".join(f"{k}={v}" for k, v in self.items())
        return f"{type(self).__name__}({fields_repr})"

    def __iter__(self) -> Iterable[str]:
        """Get the field names.

        :return: An iterable of string values.
        """
        yield from self._mapper.keys()

    def get_one(self, name: str, return_diff: bool = True) -> Union[FieldDiff, Any, list[FieldDiff], list[Any]]:
        """Get first field diff with given name.

        :param name: The name of the field diff.
        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A ``FieldDiff`` instance.
        """
        type_, names = self._mapper[name]
        if not issubclass(type_, IncrementalFieldDiff):
            return self._get_one(names[0], return_diff)
        return [self._get_one(name, return_diff) for name in names]

    def _get_one(self, name: str, return_diff: bool) -> Union[FieldDiff, Any]:
        diff = getattr(self, name)
        if return_diff:
            return diff
        return diff.value

    def get_all(self, return_diff: bool = True) -> dict[str, Union[FieldDiff, Any, list[FieldDiff], list[Any]]]:
        """Get a dictionary containing all names as keys and all the values of each one as values.

        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A ``dict`` with ``str`` keys and ``list[Any]`` values.
        """
        return {key: self.get_one(key, return_diff) for key in self.keys()}

    def flatten_items(self) -> Iterator[tuple[str, FieldDiff]]:
        """Get the field differences in a flatten way.

        :return: An ``Iterator`` of ``tuple[str, FieldDiff]`` instances.
        """
        return map(lambda diff: (diff.name, diff), self.flatten_values())

    def flatten_values(self) -> Iterator[FieldDiff]:
        """Get the field differences in a flatten way.

        :return: An ``Iterator`` of ``FieldDiff`` instances.
        """
        iterable = (v if isinstance(v, list) else [v] for v in self.values())
        return chain.from_iterable(iterable)

    @classmethod
    def from_difference(cls, a: Model, b: Model, ignore: set[str] = frozenset()) -> FieldDiffContainer:
        """Build a new instance from the difference between two models.

        :param a: Latest model instance.
        :param b: Oldest model instance.
        :param ignore: Set of fields to be ignored.
        :return: A new ``FieldDiffContainer`` instance.
        """
        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")
        differences = cls._diff(a.fields, b.fields)
        differences = [difference for difference in differences if difference.name not in ignore]
        return cls(differences)

    @staticmethod
    def _diff(a: dict[str, Field], b: dict[str, Field]) -> list[FieldDiff]:
        from ..declarative import (
            EntitySet,
            EntitySetDiff,
            ValueObjectSet,
            ValueObjectSetDiff,
        )

        differences = list()
        for a_name, a_field in a.items():
            if a_name not in b or a_field != b[a_name]:
                if isinstance(a_field.value, EntitySet):
                    diffs = EntitySetDiff.from_difference(a_field.value, b[a_name].value).diffs
                    for diff in diffs:
                        differences.append(
                            IncrementalFieldDiff(a_name, get_args(a_field.type)[0], diff.entity, diff.action)
                        )
                elif isinstance(a_field.value, ValueObjectSet):
                    diffs = ValueObjectSetDiff.from_difference(a_field.value, b[a_name].value).diffs
                    for diff in diffs:
                        differences.append(
                            IncrementalFieldDiff(a_name, get_args(a_field.type)[0], diff.entity, diff.action)
                        )
                else:
                    differences.append(FieldDiff(a_name, a_field.type, a_field.value))

        return differences

    @classmethod
    def from_model(cls, model: Model, ignore: set[str] = frozenset()) -> FieldDiffContainer:
        """Build a new difference from a single model.

        :param model: The model instance.
        :param ignore: Set of fields to be ignored.
        :return: A new ``FieldDiffContainer`` instance.
        """
        differences = list()
        for field in model.fields.values():
            differences.append(FieldDiff(field.name, field.type, field.value))

        differences = [difference for difference in differences if difference.name not in ignore]
        return cls(differences)

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())


def _build_mapper(fields: dict[str, Field]) -> dict[str, tuple[type, list[str]]]:
    mapper = defaultdict(list)
    for name, field in fields.items():
        mapper[field.value.name].append(name)
    mapper = dict(mapper)

    ans = dict()
    for k, names in mapper.items():
        types = {type(fields[name].value) for name in names}
        if len(types) > 1:
            raise ValueError(f"Multiple {FieldDiff.__name__!r} types have been provided to {k!r}: {types}")
        type_ = next(iter(types))
        if len(names) > 1 and not issubclass(type_, IncrementalFieldDiff):
            raise ValueError(f"Only {IncrementalFieldDiff.__name__!r} type allow multiple {k!r} values.")
        ans[k] = (type_, names)

    return ans
