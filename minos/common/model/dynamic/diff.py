"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from typing import (
    TYPE_CHECKING,
    Optional,
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


class FieldsDiff(BucketModel):
    """Aggregate Difference class."""

    @classmethod
    def from_difference(cls, a: Model, b: Model, ignore: Optional[list[str]] = None) -> FieldsDiff:
        """Build an ``FieldsDiff`` instance from the difference of two aggregates.

        :param a: One ``Aggregate`` instance.
        :param b: Another ``Aggregate`` instance.
        :param ignore: List of fields to be ignored.
        :return: An ``FieldsDiff`` instance.
        """
        if ignore is None:
            ignore = list()

        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")
        fields = cls._diff(a.fields, b.fields)
        for name in ignore:
            fields.pop(name, None)

        return cls(fields)

    @staticmethod
    def _diff(a: dict[str, Field], b: dict[str, Field]) -> dict[str, Field]:
        """Compute the difference between ``a`` and ``b``.

        The implemented approach is equivalent to `dict(set(a.items()) -`set(b.items()))` but without requesting any
        hashing assumptions.

        :param a: The first dictionary of fields.
        :param b: The second dictionary of fields.
        :return: The differences dictionary between ``a`` and ``b``.
        """

        def _condition(key: str) -> bool:
            return key not in b or a[key] != b[key]

        return {key: a[key] for key in a if _condition(key)}

    @classmethod
    def from_model(cls, aggregate: Model, ignore: Optional[list[str]] = None) -> FieldsDiff:
        """Build an ``FieldsDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param ignore: List of fields to be ignored.
        :return: An ``FieldsDiff`` instance.
        """
        if ignore is None:
            ignore = list()

        fields = dict(aggregate.fields)
        for name in ignore:
            fields.pop(name, None)

        return cls(fields)

    @classmethod
    def simplify(cls, *args: FieldsDiff) -> FieldsDiff:
        """Simplify an iterable of aggregate differences into a single one.

        :param args: A sequence of ``FieldsDiff` instances.
        :return: An ``FieldsDiff`` instance.
        """
        current = cls(args[0].fields)
        for another in args[1:]:
            current._fields |= another._fields
        return current
