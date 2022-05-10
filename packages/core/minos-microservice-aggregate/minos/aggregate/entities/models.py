from __future__ import (
    annotations,
)

import logging
from datetime import (
    datetime,
)
from typing import (
    Type,
    TypeVar,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_DATETIME,
    NULL_UUID,
    DeclarativeModel,
)

from ..deltas import (
    Delta,
    IncrementalFieldDiff,
)

logger = logging.getLogger(__name__)


class Entity(DeclarativeModel):
    """Entity class."""

    uuid: UUID

    version: int
    created_at: datetime
    updated_at: datetime

    def __init__(
        self,
        *args,
        uuid: UUID = NULL_UUID,
        version: int = 0,
        created_at: datetime = NULL_DATETIME,
        updated_at: datetime = NULL_DATETIME,
        **kwargs,
    ):

        super().__init__(uuid, version, created_at, updated_at, *args, **kwargs)

    def diff(self, another: Entity) -> Delta:
        """Compute the difference with another instance.

        Both ``Entity`` instances (``self`` and ``another``) must share the same ``uuid`` value.

        :param another: Another ``Entity`` instance.
        :return: An ``FieldDiffContainer`` instance.
        """
        return Delta.from_difference(self, another)

    def apply_diff(self, delta: Delta) -> None:
        """Apply the differences over the instance.

        :param delta: The ``FieldDiffContainer`` containing the values to be set.
        :return: This method does not return anything.
        """
        if self.uuid != delta.uuid:
            raise ValueError(
                f"To apply the difference, it must have same uuid. " f"Expected: {self.uuid!r} Obtained: {delta.uuid!r}"
            )

        logger.debug(f"Applying {delta!r} to {self!r}...")
        for diff in delta.fields_diff.flatten_values():
            if isinstance(diff, IncrementalFieldDiff):
                container = getattr(self, diff.name)
                if diff.action.is_delete:
                    container.discard(diff.value)
                else:
                    container.add(diff.value)
            else:
                setattr(self, diff.name, diff.value)
        self.version = delta.version
        self.updated_at = delta.created_at

    @classmethod
    def from_diff(cls: Type[T], delta: Delta, *args, **kwargs) -> T:
        """Build a new instance from an ``Delta``.

        :param delta: The difference that contains the data.
        :param args: Additional positional arguments.
        :param kwargs: Additional named arguments.
        :return: A new ``Entity`` instance.
        """
        return cls(
            *args,
            uuid=delta.uuid,
            version=delta.version,
            created_at=delta.created_at,
            updated_at=delta.created_at,
            **delta.get_fields(),
            **kwargs,
        )


T = TypeVar("T", bound=Entity)
