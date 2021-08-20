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
    Any,
)
from uuid import (
    UUID,
)

from ...actions import (
    Action,
)
from ...dynamic import (
    FieldDiff,
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
    fields_diff: FieldDiffContainer

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            if item != "fields_diff" and item in self.fields_diff:
                return self.fields_diff.get_one_value(item)
            raise exc

    def get_one_value_dict(self) -> dict[str, Any]:
        """Get a dictionary containing all names as keys and the first value of each one as values.

        :return: A ``dict`` with ``str`` keys and ``Any`` values.
        """
        return self.fields_diff.get_one_value_dict()

    def get_one_value(self, name: str) -> Any:
        """Get first value with given name.

        :param name: The name of the value.
        :return: A ``object`` instance.
        """
        return self.fields_diff.get_one_value(name)

    def get_one_dict(self) -> dict[str, FieldDiff]:
        """Get a dictionary containing all names as keys and the first field diff of each one as values.

        :return: A ``dict`` with ``str`` keys and ``FieldDiff`` values.
        """
        return self.fields_diff.get_one_dict()

    def get_one(self, name: str) -> FieldDiff:
        """Get first field diff with given name.

        :param name: The name of the field diff.
        :return: A ``FieldDiff`` instance.
        """
        return self.fields_diff.get_one(name)

    def get_all_values_dict(self) -> dict[str, list[Any]]:
        """Get a dictionary containing all names as keys and all the values of each one as values.

        :return: A ``dict`` with ``str`` keys and ``list[Any]`` values.
        """
        return self.fields_diff.get_all_values_dict()

    def get_all_values(self, name: str) -> list[Any]:
        """Get all values with given name.

        :param name: The name of the values.
        :return: A list of ``object`` instances.
        """
        return self.fields_diff.get_all_values(name)

    def get_all_dict(self) -> dict[str, list[FieldDiff]]:
        """Get a dictionary containing all names as keys and all the field diffs of each one as values.

        :return: A ``dict`` with ``str`` keys and ``list[Any]`` values.
        """
        return self.fields_diff.get_all_dict()

    def get_all(self, name: str) -> list[FieldDiff]:
        """Get all field diffs with given name.

        :param name: The name of the field diffs.
        :return: A list of ``FieldDiff`` instances.
        """
        return self.fields_diff.get_all(name)

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

        fields_diff = FieldDiffContainer.from_difference(a, b, ignore={"uuid", "version"})

        return cls(new.uuid, new.classname, new.version, action, fields_diff)

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate, action: Action = Action.CREATE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """

        fields_diff = FieldDiffContainer.from_model(aggregate, ignore={"uuid", "version"})
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, action, fields_diff)

    @classmethod
    def from_deleted_aggregate(cls, aggregate: Aggregate, action: Action = Action.DELETE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, action, FieldDiffContainer.empty())

    def decompose(self) -> list[AggregateDiff]:
        """Decompose AggregateDiff Fields into AggregateDiff with once Field.

        :return: An list of``AggregateDiff`` instances.
        """
        return [
            AggregateDiff(self.uuid, self.name, self.version, self.action, FieldDiffContainer([diff]))
            for diff in self.fields_diff.flatten_values()
        ]
