"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
)
from uuid import (
    UUID,
)

from ...dynamic import (
    FieldsDiff,
)
from ..abc import (
    DeclarativeModel,
)

if TYPE_CHECKING:
    from .model import (
        Aggregate,
    )

logger = logging.getLogger(__name__)


class AggregateDiff(DeclarativeModel):
    """Aggregate Difference class."""

    uuid: UUID
    name: str
    version: int
    fields_diff: FieldsDiff

    @classmethod
    def from_difference(cls, a: Aggregate, b: Aggregate) -> AggregateDiff:
        """Build an ``AggregateDiff`` instance from the difference of two aggregates.

        :param a: One ``Aggregate`` instance.
        :param b: Another ``Aggregate`` instance.
        :return: An ``AggregateDiff`` instance.
        """
        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")

        if a.uuid != b.uuid:
            raise ValueError(
                f"To compute aggregate differences, both arguments must have same identifier. "
                f"Obtained: {a.uuid!r} vs {b.uuid!r}"
            )

        old, new = sorted([a, b], key=attrgetter("version"))

        fields_diff = FieldsDiff.from_difference(a, b, ignore=["uuid", "version"])

        return cls(new.uuid, new.classname, new.version, fields_diff)

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :return: An ``AggregateDiff`` instance.
        """

        fields_diff = FieldsDiff.from_model(aggregate, ignore=["uuid", "version"])
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, fields_diff)

    @classmethod
    def from_deleted_aggregate(cls, aggregate: Aggregate) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :return: An ``AggregateDiff`` instance.
        """
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, FieldsDiff.empty())

    @classmethod
    def simplify(cls, *args: AggregateDiff) -> AggregateDiff:
        """Simplify an iterable of aggregate differences into a single one.

        :param args: A sequence of ``FieldsDiff` instances.
        :return: An ``FieldsDiff`` instance.
        """
        args = sorted(args, key=attrgetter("version"))

        current = dict()
        for another in map(attrgetter("fields_diff"), args):
            # noinspection PyUnresolvedReferences
            current |= another.fields

        return cls(args[-1].uuid, args[-1].name, args[-1].version, FieldsDiff(current))
