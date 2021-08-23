"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from datetime import (
    datetime,
)
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Type,
    Union,
)
from uuid import (
    UUID,
)

from ..importlib import (
    import_module,
)

if TYPE_CHECKING:
    from ..model import (
        Action,
        Aggregate,
        AggregateDiff,
    )


class RepositoryEntry:
    """Class that represents an entry (or row) on the events repository database which stores the aggregate changes."""

    __slots__ = "aggregate_uuid", "aggregate_name", "version", "data", "id", "action", "created_at"

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        aggregate_uuid: UUID,
        aggregate_name: str,
        version: int,
        data: Union[bytes, memoryview] = bytes(),
        id: Optional[int] = None,
        action: Optional[Union[str, Action]] = None,
        created_at: Optional[datetime] = None,
    ):
        if isinstance(data, memoryview):
            data = data.tobytes()
        if action is not None and isinstance(action, str):
            from ..model import (
                Action,
            )

            action = Action.value_of(action)

        self.aggregate_uuid = aggregate_uuid
        self.aggregate_name = aggregate_name
        self.version = version
        self.data = data

        self.id = id
        self.action = action
        self.created_at = created_at

    @classmethod
    def from_aggregate_diff(cls, aggregate_diff: AggregateDiff) -> RepositoryEntry:
        """Build a new instance from an ``Aggregate``.

        :param aggregate_diff: The aggregate difference.
        :return: A new ``RepositoryEntry`` instance.
        """
        # noinspection PyTypeChecker
        return cls(
            aggregate_diff.uuid, aggregate_diff.name, aggregate_diff.version, aggregate_diff.fields_diff.avro_bytes
        )

    @property
    def aggregate_cls(self) -> Type[Aggregate]:
        """Load the concrete ``Aggregate`` class.

        :return: A ``Type`` object.
        """
        # noinspection PyTypeChecker
        return import_module(self.aggregate_name)

    @property
    def aggregate_diff(self) -> AggregateDiff:
        """Get the stored ``AggregateDiff`` instance.

        :return: An ``AggregateDiff`` instance.
        """
        from ..model import (
            AggregateDiff,
            FieldDiffContainer,
        )

        return AggregateDiff(
            self.aggregate_uuid,
            self.aggregate_name,
            self.version,
            self.action,
            self.created_at,
            FieldDiffContainer.from_avro_bytes(self.data),
        )

    def __eq__(self, other: "RepositoryEntry") -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.aggregate_uuid,
            self.aggregate_name,
            self.version,
            self.data,
            self.id,
            self.action,
            self.created_at,
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"aggregate_uuid={self.aggregate_uuid!r}, aggregate_name={self.aggregate_name!r}, "
            f"version={self.version!r}, data={self.data!r}, "
            f"id={self.id!r}, action={self.action!r}, created_at={self.created_at!r})"
        )
