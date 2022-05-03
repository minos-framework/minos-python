from __future__ import (
    annotations,
)

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
    import_module,
)
from minos.transactions import (
    TransactionEntry,
)

from ..actions import (
    Action,
)
from .fields import (
    FieldDiffContainer,
)
from .models import (
    Delta,
)

if TYPE_CHECKING:
    from ..entities import (
        Entity,
    )


class DeltaEntry:
    """Class that represents an entry (or row) on the delta repository database which stores the root entity changes."""

    __slots__ = (
        "uuid",
        "name",
        "version",
        "data",
        "id",
        "action",
        "created_at",
        "transaction_uuid",
    )

    # noinspection PyShadowingBuiltins
    def __init__(
        self,
        uuid: UUID,
        name: str,
        version: Optional[int] = None,
        data: Union[bytes, memoryview] = bytes(),
        id: Optional[int] = None,
        action: Optional[Union[str, Action]] = None,
        created_at: Optional[datetime] = None,
        transaction_uuid: UUID = NULL_UUID,
    ):
        if isinstance(data, memoryview):
            data = data.tobytes()
        if action is not None and isinstance(action, str):
            action = Action.value_of(action)

        self.uuid = uuid
        self.name = name
        self.version = version
        self.data = data

        self.id = id
        self.action = action
        self.created_at = created_at
        self.transaction_uuid = transaction_uuid

    @classmethod
    def from_delta(cls, delta: Delta, *, transaction: Optional[TransactionEntry] = None, **kwargs) -> DeltaEntry:
        """Build a new instance from a ``Entity``.

        :param delta: The delta.
        :param transaction: Optional transaction.
        :param kwargs: Additional named arguments.
        :return: A new ``DeltaEntry`` instance.
        """
        if transaction is not None:
            kwargs["transaction_uuid"] = transaction.uuid

        # noinspection PyTypeChecker
        return cls(
            uuid=delta.uuid,
            name=delta.name,
            data=delta.fields_diff.avro_bytes,
            action=delta.action,
            **kwargs,
        )

    @classmethod
    def from_another(cls, another: DeltaEntry, **kwargs) -> DeltaEntry:
        """Build a new instance from another ``DeltaEntry``.

        :param another: The ``DeltaEntry``.
        :param kwargs: Additional named arguments.
        :return: A new ``DeltaEntry`` instance.
        """
        return cls(**(another.as_raw() | kwargs | {"id": None}))

    def as_raw(self) -> dict[str, Any]:
        """Get a raw representation of the instance.

        :return: A dictionary in which the keys are attribute names and values the attribute contents.
        """
        return {
            "uuid": self.uuid,
            "name": self.name,
            "version": self.version,
            "data": self.data,
            "id": self.id,
            "action": self.action.value,
            "created_at": self.created_at,
            "transaction_uuid": self.transaction_uuid,
        }

    @property
    def type_(self) -> type[Entity]:
        """Load the concrete ``Entity`` class.

        :return: A ``Type`` object.
        """
        # noinspection PyTypeChecker
        return import_module(self.name)

    @property
    def delta(self) -> Delta:
        """Get the stored ``Delta`` instance.

        :return: An ``Delta`` instance.
        """
        return Delta(
            self.uuid,
            self.name,
            self.version,
            self.action,
            self.created_at,
            self.field_diff_container,
        )

    @property
    def field_diff_container(self) -> FieldDiffContainer:
        """Get the stored field diff container.

        :return: A ``FieldDiffContainer`` instance.
        """
        if not self.data:
            return FieldDiffContainer.empty()

        return FieldDiffContainer.from_avro_bytes(self.data)

    def __eq__(self, other: "DeltaEntry") -> bool:
        return type(self) == type(other) and tuple(self) == tuple(other)

    def __hash__(self) -> int:
        return hash(tuple(self))

    def __iter__(self) -> Iterable:
        yield from (
            self.uuid,
            self.name,
            self.version,
            self.data,
            self.id,
            self.action,
            self.created_at,
            self.transaction_uuid,
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"uuid={self.uuid!r}, name={self.name!r}, "
            f"version={self.version!r}, len(data)={len(self.data)!r}, "
            f"id={self.id!r}, action={self.action!r}, created_at={self.created_at!r}, "
            f"transaction_uuid={self.transaction_uuid!r})"
        )
