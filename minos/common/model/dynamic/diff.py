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
    TYPE_CHECKING,
    Any,
    Generic,
    Optional,
    TypeVar,
)
from uuid import (
    uuid4,
)

from ..actions import (
    Action,
)
from ..declarative import (
    DeclarativeModel,
)
from .bucket import (
    BucketModel,
)

if TYPE_CHECKING:
    from ..abc import (
        Model,
    )
    from ..fields import (
        Field,
    )

logger = logging.getLogger(__name__)

T = TypeVar("T")


class Difference(DeclarativeModel, Generic[T]):
    """TODO"""

    name: str
    value: T


class IncrementalDifference(Difference, Generic[T]):
    """TODO"""

    action: Action


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

        def _condition(key: str) -> bool:
            return key not in b or a[key] != b[key]

        fields = {key: a[key] for key in a if _condition(key)}

        differences = list()
        for field in fields.values():
            differences.append(Difference(field.name, field.value))

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
            differences.append(Difference(field.name, field.value))

        differences = [difference for difference in differences if difference.name not in ignore]
        return cls(differences)

    @staticmethod
    def generate_random_str() -> str:
        """Generate a random string

        :return: A random string value.
        """
        return str(uuid4())
