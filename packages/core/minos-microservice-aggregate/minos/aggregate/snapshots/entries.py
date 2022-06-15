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
    AvroDataDecoder,
    AvroSchemaDecoder,
    MinosJsonBinaryProtocol,
    classname,
    import_module,
)

from ..deltas import (
    DeltaEntry,
)
from ..exceptions import (
    AlreadyDeletedException,
)

if TYPE_CHECKING:
    from ..entities import (
        Entity,
    )


class SnapshotEntry:
    """Minos Snapshot Entry class.

    Is the python object representation of a row in the ``snapshot`` storage system.
    """

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        uuid: UUID,
        type_: Union[str, type[Entity]],
        version: int,
        schema: Optional[Union[list[dict[str, Any]], dict[str, Any]], bytes, memoryview] = None,
        data: Optional[Union[dict[str, Any], str]] = None,
        created_at: Optional[datetime] = None,
        updated_at: Optional[datetime] = None,
        transaction_uuid: UUID = NULL_UUID,
        deleted: bool = False,
        **kwargs,
    ):
        if isinstance(type_, str):
            type_ = import_module(type_)
        if isinstance(schema, memoryview):
            schema = schema.tobytes()
        if isinstance(schema, bytes):
            schema = MinosJsonBinaryProtocol.decode(schema)

        if isinstance(data, str):
            data = json.loads(data)

        if not deleted and kwargs:
            if not data:
                data = kwargs.copy()
            else:
                data = data.copy()
                data |= kwargs

        self.uuid = uuid
        self.type_ = type_
        self.version = version

        self.schema = schema
        self.data = data

        self.created_at = created_at
        self.updated_at = updated_at

        self.transaction_uuid = transaction_uuid

    @classmethod
    def from_entity(cls, instance: Entity, **kwargs) -> SnapshotEntry:
        """Build a new instance from a ``Entity``.

        :param instance: The ``Entity`` instance.
        :return: A new ``SnapshotEntry`` instance.
        """
        data = {k: v for k, v in instance.avro_data.items() if k not in {"uuid", "version", "created_at", "updated_at"}}

        # noinspection PyTypeChecker
        return cls(
            uuid=instance.uuid,
            type_=instance.classname,
            version=instance.version,
            schema=instance.avro_schema,
            data=data,
            created_at=instance.created_at,
            updated_at=instance.updated_at,
            **kwargs,
        )

    @classmethod
    def from_delta_entry(cls, entry: DeltaEntry) -> SnapshotEntry:
        """Build a new ``SnapshotEntry`` from a deletion delta.

        :param entry: The delta entry containing the delete information.
        :return: A new ``SnapshotEntry`` instance.
        """
        return cls(
            uuid=entry.uuid,
            type_=entry.type_,
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
            "type_": classname(self.type_),
            "version": self.version,
            "schema": self.schema,
            "data": self.data,
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

    def build(self, **kwargs) -> Entity:
        """Rebuild the stored ``Entity`` object instance from the internal state.

        :param kwargs: Additional named arguments.
        :return: A ``Entity`` instance.
        """

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

        type_ = self.build_type()

        data_decoder = AvroDataDecoder()
        instance = data_decoder.build(data, type_)

        return instance

    def build_type(self) -> type[Entity]:
        """Build the entity type.

        :return: A ``type`` instance.
        """

        if self.schema:
            schema_decoder = AvroSchemaDecoder()
            type_ = schema_decoder.build(self.schema)
        else:
            type_ = self.type_

        # noinspection PyTypeChecker
        return type_

    def __eq__(self, other: SnapshotEntry) -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __iter__(self) -> Iterable:
        # noinspection PyRedundantParentheses
        yield from (
            self.type_,
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
            f"{name}(uuid={self.uuid!r}, type_={self.type_!r}, "
            f"version={self.version!r}, schema={self.schema!r}, data={self.data!r}, "
            f"created_at={self.created_at!r}, updated_at={self.updated_at!r}, "
            f"transaction_uuid={self.transaction_uuid!r})"
        )
