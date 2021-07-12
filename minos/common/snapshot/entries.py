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

from ..exceptions import (
    MinosSnapshotDeletedAggregateException,
)
from ..importlib import (
    import_module,
)

if TYPE_CHECKING:
    from ..model import (
        Aggregate,
    )


class SnapshotEntry:
    """Minos Snapshot Entry class.

    Is the python object representation of a row in the ``snapshot`` storage system.
    """

    __slots__ = "aggregate_uuid", "aggregate_name", "version", "data", "created_at", "updated_at"

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        aggregate_uuid: UUID,
        aggregate_name: str,
        version: int,
        data: Union[bytes, memoryview, None] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
    ):
        if isinstance(data, memoryview):
            data = data.tobytes()

        self.aggregate_uuid = aggregate_uuid
        self.aggregate_name = aggregate_name
        self.version = version
        self.data = data

        self.created_at = created_at
        self.updated_at = updated_at

    @classmethod
    def from_aggregate(cls, aggregate: Aggregate) -> SnapshotEntry:
        """Build a new instance from an ``Aggregate``.

        :param aggregate: The aggregate instance.
        :return: A new ``MinosSnapshotEntry`` instance.
        """
        # noinspection PyTypeChecker
        return cls(aggregate.uuid, aggregate.classname, aggregate.version, aggregate.avro_bytes)

    @property
    def aggregate(self) -> Aggregate:
        """Rebuild the stored ``Aggregate`` object instance from the internal state.

        :return: A ``Aggregate`` instance.
        """
        if self.data is None:
            raise MinosSnapshotDeletedAggregateException(
                f"The {self.aggregate_uuid!r} id points to an already deleted aggregate."
            )
        cls = self.aggregate_cls
        instance = cls.from_avro_bytes(self.data, id=self.aggregate_uuid, version=self.version)
        return instance

    @property
    def aggregate_cls(self) -> Type[Aggregate]:
        """Load the concrete ``Aggregate`` class.

        :return: A ``Type`` object.
        """
        # noinspection PyTypeChecker
        return import_module(self.aggregate_name)

    def __eq__(self, other: SnapshotEntry) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (self.aggregate_name, self.version, self.data, self.created_at, self.updated_at)

    def __repr__(self):
        name = type(self).__name__
        return (
            f"{name}(aggregate_uuid={self.aggregate_uuid!r}, aggregate_name={self.aggregate_name!r}, "
            f"version={self.version!r}, data={self.data!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r})"
        )
