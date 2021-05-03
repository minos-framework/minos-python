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
from enum import (
    Enum,
)
from typing import (
    TYPE_CHECKING,
    Iterable,
    Optional,
    Union,
)

from ..exceptions import (
    MinosRepositoryUnknownActionException,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class MinosRepositoryAction(Enum):
    """Enum class that describes the available repository actions."""

    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"

    @classmethod
    def value_of(cls, value: str) -> Optional[MinosRepositoryAction]:
        """Get the action based on its text representation."""
        for item in cls.__members__.values():
            if item.value == value:
                return item
        raise MinosRepositoryUnknownActionException(
            f"The given value does not match with any enum items. Obtained {value}"
        )


class MinosRepositoryEntry(object):
    """Class that represents an entry (or row) on the events repository database which stores the aggregate changes."""

    __slots__ = "aggregate_id", "aggregate_name", "version", "data", "id", "action", "created_at"

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        aggregate_id: int,
        aggregate_name: str,
        version: int,
        data: Union[bytes, memoryview] = bytes(),
        id: Optional[int] = None,
        action: Optional[Union[str, MinosRepositoryAction]] = None,
        created_at: Optional[datetime] = None,
    ):
        if isinstance(data, memoryview):
            data = data.tobytes()
        if action is not None and isinstance(action, str):
            action = MinosRepositoryAction.value_of(action)

        self.aggregate_id = aggregate_id
        self.aggregate_name = aggregate_name
        self.version = version
        self.data = data

        self.id = id
        self.action = action
        self.created_at = created_at

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate) -> MinosRepositoryEntry:
        """Build a new instance from an ``Aggregate``.

        :param aggregate: The aggregate instance.
        :return: A new ``MinosRepositoryEntry`` instance.
        """
        # noinspection PyTypeChecker
        return cls(aggregate.id, aggregate.classname, aggregate.version, aggregate.avro_bytes)

    def __eq__(self, other: "MinosRepositoryEntry") -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.aggregate_id,
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
            f"aggregate_id={repr(self.aggregate_id)}, aggregate_name={repr(self.aggregate_name)}, "
            f"version={repr(self.version)}, data={repr(self.data)}, "
            f"id={repr(self.id)}, action={repr(self.action)}, created_at={repr(self.created_at)})"
        )
