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
from typing import (
    Any,
    Generic,
    Optional,
    Type,
    TypeVar,
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


class Difference(Model, Generic[T]):
    """TODO"""

    name: str
    value: T

    @classmethod
    def from_model_type(cls, model_type: ModelType, *args, **kwargs) -> Difference:
        """TODO"""
        kwargs["type_"] = model_type.type_hints["value"]
        return cls(*args, **kwargs)

    def __init__(self, name: str, type_: Type, value: Any):
        super().__init__([Field("name", str, name), Field("value", type_, value)])


class IncrementalDifference(Difference, Generic[T]):
    """TODO"""

    name: str
    value: T
    action: Action

    @classmethod
    def from_model_type(cls, model_type: ModelType, *args, **kwargs) -> IncrementalDifference:
        """TODO"""
        kwargs["type_"] = model_type.type_hints["value"]
        return cls(*args, **kwargs)

    def __init__(self, name: str, type_: Type, value: Any, action: Action):
        Model.__init__(self, [Field("name", str, name), Field("value", type_, value), Field("action", Action, action)])


class DifferenceContainer(BucketModel):
    """TODO"""

    def __init__(self, fields: list[Difference], *args, **kwargs):
        if isinstance(fields, list):
            from ..fields import (
                Field,
            )

            fields = [Field(self.generate_random_str(), Difference, v) for v in fields]
        super().__init__(fields, **kwargs)

        mapper = defaultdict(list)
        for name, field in self._fields.items():
            mapper[field.value.name].append(name)
        self._name_mapper = mapper

    def __getattr__(self, item: str) -> Any:
        if item in self._name_mapper:
            values = [getattr(self, name) for name in self._name_mapper.get(item)]
            if len(values) == 1:
                return values[0]
            return values
        else:
            return super().__getattr__(item)

    def __eq__(self, other):
        return type(self) == type(other) and self.differences == other.differences

    @property
    def differences(self) -> dict[str, Difference]:
        """TODO

        :return: TODO
        """
        return {k: [getattr(self, name) for name in vs] for k, vs in self._name_mapper.items()}

    def __repr__(self) -> str:
        fields_repr = ", ".join(f"{name}={getattr(self, name)}" for name in self._name_mapper.keys())
        return f"{type(self).__name__}({fields_repr})"

    @classmethod
    def from_difference(cls, a: Model, b: Model, ignore: Optional[set[str]] = None) -> DifferenceContainer:
        """TODO

        :param a: TODO
        :param b: TODO
        :param ignore: TODO
        :return: TODO
        """
        if ignore is None:
            ignore = set()
        ignore = set(ignore)

        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")
        differences = cls._diff(a.fields, b.fields)
        differences = [difference for difference in differences if difference.name not in ignore]
        return cls(differences)

    @staticmethod
    def _diff(a: dict[str, Field], b: dict[str, Field]) -> list[Difference]:
        """TODO

        :param a: TODO
        :param b: TODO
        :return: TODO
        """
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
                            IncrementalDifference(a_name, get_args(a_field.type)[0], diff.entity, diff.action)
                        )
                elif isinstance(a_field.value, ValueObjectSet):
                    diffs = ValueObjectSetDiff.from_difference(a_field.value, b[a_name].value).diffs
                    for diff in diffs:
                        differences.append(
                            IncrementalDifference(a_name, get_args(a_field.type)[0], diff.entity, diff.action)
                        )
                else:
                    differences.append(Difference(a_name, a_field.type, a_field.value))

        return differences

    @classmethod
    def from_model(cls, aggregate: Model, ignore: Optional[set[str]] = None) -> DifferenceContainer:
        """TODO

        :param aggregate: TODO
        :param ignore: TODO
        :return: TODO
        """
        if ignore is None:
            ignore = set()
        ignore = set(ignore)

        differences = list()
        for field in aggregate.fields.values():
            differences.append(Difference(field.name, field.type, field.value))

        differences = [difference for difference in differences if difference.name not in ignore]
        return cls(differences)

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())
