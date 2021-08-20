"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

import logging
from datetime import (
    datetime,
)
from operator import (
    attrgetter,
)
from typing import (
    TYPE_CHECKING,
    Any,
)
from uuid import (
    UUID,
)

from ...actions import (
    Action,
)
from ...dynamic import (
    FieldDiffContainer,
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
    """Aggregate Diff class."""

    uuid: UUID
    name: str
    version: int
    action: Action
    created_at: datetime

    fields_diff: FieldDiffContainer

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            if item != "fields_diff":
                return getattr(self.fields_diff, item).value
            raise exc

    @classmethod
    def from_difference(cls, a: Aggregate, b: Aggregate, action: Action = Action.UPDATE) -> AggregateDiff:
        """Build an ``AggregateDiff`` instance from the difference of two aggregates.

        :param a: One ``Aggregate`` instance.
        :param b: Another ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """
        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")

        if a.uuid != b.uuid:
            raise ValueError(
                f"To compute aggregate differences, both arguments must have same identifier. "
                f"Obtained: {a.uuid!r} vs {b.uuid!r}"
            )

        old, new = sorted([a, b], key=attrgetter("version"))

        fields_diff = FieldDiffContainer.from_difference(a, b, ignore={"uuid", "version", "created_at", "updated_at"})

        return cls(new.uuid, new.classname, new.version, action, new.updated_at, fields_diff)

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate, action: Action = Action.CREATE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """

        fields_diff = FieldDiffContainer.from_model(aggregate, ignore={"uuid", "version", "created_at", "updated_at"})
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, action, aggregate.updated_at, fields_diff)

    @classmethod
    def from_deleted_aggregate(cls, aggregate: Aggregate, action: Action = Action.DELETE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """
        return cls(
            aggregate.uuid,
            aggregate.classname,
            aggregate.version,
            action,
            aggregate.updated_at,
            FieldDiffContainer.empty(),
        )

    def decompose(self) -> list[AggregateDiff]:
        """Decompose AggregateDiff Fields into AggregateDiff with once Field.

        :return: An list of``AggregateDiff`` instances.
        """
        return [
            AggregateDiff(self.uuid, self.name, self.version, self.action, self.created_at, FieldDiffContainer([diff]))
            for diff in self.fields_diff.values()
        ]
