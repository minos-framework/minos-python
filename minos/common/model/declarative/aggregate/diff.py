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
    Union,
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
    from ....repository import (
        RepositoryEntry,
    )
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
            try:
                return self.get_one(item)
            except Exception:
                raise exc

    def get_one(self, name: str, return_diff: bool = False) -> Union[FieldDiff, Any, list[FieldDiff], list[Any]]:
        """Get first field diff with given name.

        :param name: The name of the field diff.
        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A ``FieldDiff`` instance.
        """
        return self.fields_diff.get_one(name, return_diff)

    def get_all(self, return_diff: bool = False) -> dict[str, Union[FieldDiff, Any, list[FieldDiff], list[Any]]]:
        """Get all field diffs with given name.

        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A list of ``FieldDiff`` instances.
        """
        return self.fields_diff.get_all(return_diff)

    def update_from_repository_entry(self, entry: RepositoryEntry) -> None:
        """Update metadata from a repository entry.

        :param entry: A repository entry.
        :return: This method does not return anything.
        """
        self.uuid = entry.aggregate_uuid
        self.version = entry.version
        self.created_at = entry.created_at

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

        return cls(
            uuid=new.uuid,
            name=new.classname,
            version=new.version,
            action=action,
            created_at=new.updated_at,
            fields_diff=fields_diff,
        )

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate, action: Action = Action.CREATE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """

        fields_diff = FieldDiffContainer.from_model(aggregate, ignore={"uuid", "version", "created_at", "updated_at"})
        return cls(
            uuid=aggregate.uuid,
            name=aggregate.classname,
            version=aggregate.version,
            action=action,
            created_at=aggregate.updated_at,
            fields_diff=fields_diff,
        )

    @classmethod
    def from_deleted_aggregate(cls, aggregate: Aggregate, action: Action = Action.DELETE) -> AggregateDiff:
        """Build an ``AggregateDiff`` from an ``Aggregate`` (considering all fields as differences).

        :param aggregate: An ``Aggregate`` instance.
        :param action: The action to that generates the aggregate difference.
        :return: An ``AggregateDiff`` instance.
        """
        return cls(
            uuid=aggregate.uuid,
            name=aggregate.classname,
            version=aggregate.version,
            action=action,
            created_at=aggregate.updated_at,
            fields_diff=FieldDiffContainer.empty(),
        )

    def decompose(self) -> list[AggregateDiff]:
        """Decompose AggregateDiff Fields into AggregateDiff with once Field.

        :return: An list of``AggregateDiff`` instances.
        """
        return [
            type(self)(
                uuid=self.uuid,
                name=self.name,
                version=self.version,
                action=self.action,
                created_at=self.created_at,
                fields_diff=FieldDiffContainer([diff]),
            )
            for diff in self.fields_diff.flatten_values()
        ]
