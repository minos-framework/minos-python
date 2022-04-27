from __future__ import (
    annotations,
)

import logging
from datetime import (
    datetime,
)
from typing import (
    Optional,
    Type,
    TypeVar,
)
from uuid import (
    UUID,
    uuid4,
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

    def __init__(self, *args, uuid: Optional[UUID] = None, **kwargs):
        if uuid is None:
            uuid = uuid4()
        super().__init__(uuid, *args, **kwargs)


class ExternalEntity(Entity):
    """External Entity class."""

    version: int

    def __init__(self, uuid: UUID, *args, **kwargs):
        super().__init__(uuid=uuid, *args, **kwargs)


T = TypeVar("T", bound="RootEntity")


class RootEntity(Entity):
    """Base Root Entity class."""

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

        super().__init__(version, created_at, updated_at, *args, uuid=uuid, **kwargs)

    def diff(self, another: RootEntity) -> Delta:
        """Compute the difference with another instance.

        Both ``RootEntity`` instances (``self`` and ``another``) must share the same ``uuid`` value.

        :param another: Another ``RootEntity`` instance.
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
        :return: A new ``RootEntity`` instance.
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
