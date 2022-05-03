from __future__ import (
    annotations,
)

import json
from datetime import (
    datetime,
)
from typing import (
    TYPE_CHECKING,
    Any,
    Iterable,
    Optional,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    MinosJsonBinaryProtocol,
    import_module,
)

from ..events import (
    EventEntry,
)
from ..exceptions import (
    AlreadyDeletedException,
)

if TYPE_CHECKING:
    from ..entities import (
        RootEntity,
    )


class SnapshotEntry:
    """Minos Snapshot Entry class.

    Is the python object representation of a row in the ``snapshot`` storage system.
    """

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        uuid: UUID,
        name: str,
        version: int,
        schema: Optional[Union[list[dict[str, Any]], dict[str, Any]], bytes, memoryview] = None,
        data: Optional[Union[dict[str, Any], str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        transaction_uuid: UUID = NULL_UUID,
    ):
        if isinstance(schema, memoryview):
            schema = schema.tobytes()
        if isinstance(schema, bytes):
            schema = MinosJsonBinaryProtocol.decode(schema)

        if isinstance(data, str):
            data = json.loads(data)

        self.uuid = uuid
        self.name = name
        self.version = version

        self.schema = schema
        self.data = data

        self.created_at = created_at
        self.updated_at = updated_at

        self.transaction_uuid = transaction_uuid

    @classmethod
    def from_root_entity(cls, instance: RootEntity, **kwargs) -> SnapshotEntry:
        """Build a new instance from a ``RootEntity``.

        :param instance: The ``RootEntity`` instance.
        :return: A new ``SnapshotEntry`` instance.
        """
        data = {k: v for k, v in instance.avro_data.items() if k not in {"uuid", "version", "created_at", "updated_at"}}

        # noinspection PyTypeChecker
        return cls(
            uuid=instance.uuid,
            name=instance.classname,
            version=instance.version,
            schema=instance.avro_schema,
            data=data,
            created_at=instance.created_at,
            updated_at=instance.updated_at,
            **kwargs,
        )

    @classmethod
    def from_event_entry(cls, entry: EventEntry) -> SnapshotEntry:
        """Build a new ``SnapshotEntry`` from a deletion event.

        :param entry: The event entry containing the delete information.
        :return: A new ``SnapshotEntry`` instance.
        """
        return cls(
            uuid=entry.uuid,
            name=entry.name,
            version=entry.version,
            created_at=entry.created_at,
            updated_at=entry.created_at,
            transaction_uuid=entry.transaction_uuid,
        )

    def as_raw(self) -> dict[str, Any]:
        """Get a raw representation of the instance.

        :return: A dictionary in which the keys are attribute names and values the attribute contents.
        """
        return {
            "uuid": self.uuid,
            "name": self.name,
            "version": self.version,
            "schema": self.encoded_schema,
            "data": self.encoded_data,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "transaction_uuid": self.transaction_uuid,
        }

    @property
    def encoded_schema(self) -> Optional[bytes]:
        """Get the encoded schema if available.

        :return: A ``bytes`` instance or ``None``.
        """
        if self.schema is None:
            return None

        return MinosJsonBinaryProtocol.encode(self.schema)

    @property
    def encoded_data(self) -> Optional[str]:
        """Get the encoded data if available.

        :return: A ``str`` instance or ``None``.
        """
        if self.data is None:
            return None

        return json.dumps(self.data)

    def build(self, **kwargs) -> RootEntity:
        """Rebuild the stored ``RootEntity`` object instance from the internal state.

        :param kwargs: Additional named arguments.
        :return: A ``RootEntity`` instance.
        """
        from ..entities import (
            RootEntity,
        )

        if self.data is None:
            raise AlreadyDeletedException(f"The {self.uuid!r} identifier belongs to an already deleted instance.")
        data = dict(self.data)
        data |= {
            "uuid": self.uuid,
            "version": self.version,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
        }
        data |= kwargs
        instance = RootEntity.from_avro(self.schema, data)
        return instance

    @property
    def type_(self) -> type[RootEntity]:
        """Load the concrete ``RootEntity`` class.

        :return: A ``Type`` object.
        """
        # noinspection PyTypeChecker
        return import_module(self.name)

    def __eq__(self, other: SnapshotEntry) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.name,
            self.version,
            self.schema,
            self.data,
            self.created_at,
            self.updated_at,
            self.transaction_uuid,
        )

    def __repr__(self):
        name = type(self).__name__
        return (
            f"{name}(uuid={self.uuid!r}, name={self.name!r}, "
            f"version={self.version!r}, schema={self.schema!r}, data={self.data!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r}, "
            f"transaction_uuid={self.transaction_uuid!r})"
        )
