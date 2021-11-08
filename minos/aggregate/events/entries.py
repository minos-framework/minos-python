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
    Type,
    Union,
)
from uuid import (
    UUID,
)

from minos.common import (
    NULL_UUID,
    import_module,
)

if TYPE_CHECKING:
    from ..models import (
        Action,
        Aggregate,
        AggregateDiff,
        FieldDiffContainer,
    )
    from ..transactions import (
        TransactionEntry,
    )


class EventEntry:
    """Class that represents an entry (or row) on the events repository database which stores the aggregate changes."""

    __slots__ = (
        "aggregate_uuid",
        "aggregate_name",
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
        aggregate_uuid: UUID,
        aggregate_name: str,
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
            from ..models import (
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
        self.transaction_uuid = transaction_uuid

    @classmethod
    def from_aggregate_diff(
        cls, aggregate_diff: AggregateDiff, *, transaction: Optional[TransactionEntry] = None, **kwargs
    ) -> EventEntry:
        """Build a new instance from an ``Aggregate``.

        :param aggregate_diff: The aggregate difference.
        :param transaction: Optional transaction.
        :param kwargs: Additional named arguments.
        :return: A new ``EventEntry`` instance.
        """
        if transaction is not None:
            kwargs["transaction_uuid"] = transaction.uuid

        # noinspection PyTypeChecker
        return cls(
            aggregate_uuid=aggregate_diff.uuid,
            aggregate_name=aggregate_diff.name,
            data=aggregate_diff.fields_diff.avro_bytes,
            action=aggregate_diff.action,
            **kwargs,
        )

    @classmethod
    def from_another(cls, another: EventEntry, **kwargs) -> EventEntry:
        """Build a new instance from another ``EventEntry``.

        :param another: The ``EventEntry``.
        :param kwargs: Additional named arguments.
        :return: A new ``EventEntry`` instance.
        """
        return cls(**(another.as_raw() | kwargs | {"id": None}))

    def as_raw(self) -> dict[str, Any]:
        """Get a raw representation of the instance.

        :return: A dictionary in which the keys are attribute names and values the attribute contents.
        """
        return {
            "aggregate_uuid": self.aggregate_uuid,
            "aggregate_name": self.aggregate_name,
            "version": self.version,
            "data": self.data,
            "id": self.id,
            "action": self.action.value,
            "created_at": self.created_at,
            "transaction_uuid": self.transaction_uuid,
        }

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
        from ..models import (
            AggregateDiff,
        )

        return AggregateDiff(
            self.aggregate_uuid,
            self.aggregate_name,
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
        from ..models import (
            FieldDiffContainer,
        )

        if not self.data:
            return FieldDiffContainer.empty()

        return FieldDiffContainer.from_avro_bytes(self.data)

    def __eq__(self, other: "EventEntry") -> bool:
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
            self.transaction_uuid,
        )

    def __repr__(self):
        return (
            f"{type(self).__name__}("
            f"aggregate_uuid={self.aggregate_uuid!r}, aggregate_name={self.aggregate_name!r}, "
            f"version={self.version!r}, len(data)={len(self.data)!r}, "
            f"id={self.id!r}, action={self.action!r}, created_at={self.created_at!r}, "
            f"transaction_uuid={self.transaction_uuid!r})"
        )
