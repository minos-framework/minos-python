from __future__ import (
    annotations,
)

import logging
import warnings
from datetime import (
    datetime,
)
from functools import (
    total_ordering,
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

from minos.common import (
    DeclarativeModel,
)

from ..actions import (
    Action,
)
from .fields import (
    FieldDiff,
    FieldDiffContainer,
)

if TYPE_CHECKING:
    from ..entities import (
        Entity,
    )

logger = logging.getLogger(__name__)


@total_ordering
class Delta(DeclarativeModel):
    """Delta class."""

    uuid: UUID
    name: str
    version: int
    action: Action
    created_at: datetime

    fields_diff: FieldDiffContainer

    @property
    def simplified_name(self) -> str:
        """Get the Entity's simplified name.

        :return: An string value.
        """
        return self.name.rsplit(".", 1)[-1]

    def __lt__(self, other: Any) -> bool:
        return isinstance(other, type(self)) and (
            (self.uuid == other.uuid and self.version < other.version) or (self.created_at < other.created_at)
        )

    def __getitem__(self, item: str) -> Any:
        try:
            return super().__getitem__(item)
        except KeyError as exc:
            if item != "fields_diff":
                try:
                    return self.get_field(item)
                except Exception:
                    raise exc
            raise exc

    def __getattr__(self, item: str) -> Any:
        try:
            return super().__getattr__(item)
        except AttributeError as exc:
            try:
                return self[item]
            except Exception:
                raise exc

    def get_one(self, name: str, return_diff: bool = False) -> Union[FieldDiff, Any, list[FieldDiff], list[Any]]:
        """Get first field diff with given name.

        :param name: The name of the field diff.
        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A ``FieldDiff`` instance.
        """
        warnings.warn("get_one() method is deprecated by get_field() and will be removed soon.", DeprecationWarning)
        return self.get_field(name, return_diff)

    def has_field(self, name: str) -> bool:
        """Check if the field identified is present.

        :param name: The name of the field.
        :return: ``True`` if the field is present or ``False`` otherwise.
        """
        return self.fields_diff.has_one(name)

    def get_field(self, name: str, return_diff: bool = False) -> Union[FieldDiff, Any, list[FieldDiff], list[Any]]:
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
        warnings.warn("get_all() method is deprecated by get_fields() and will be removed soon.", DeprecationWarning)
        return self.get_fields(return_diff)

    def get_fields(self, return_diff: bool = False) -> dict[str, Union[FieldDiff, Any, list[FieldDiff], list[Any]]]:
        """Get all field diffs with given name.

        :param return_diff: If ``True`` the result is returned as field diff instances, otherwise the result is
            returned as value instances.
        :return: A list of ``FieldDiff`` instances.
        """
        return self.fields_diff.get_all(return_diff)

    @classmethod
    def from_difference(cls, a: Entity, b: Entity, action: Action = Action.UPDATE) -> Delta:
        """Build an ``Delta`` instance from the difference of two instances.

        :param a: One ``Entity`` instance.
        :param b: Another ``Entity`` instance.
        :param action: The action that generates the ``Entity`` difference.
        :return: An ``Delta`` instance.
        """
        logger.debug(f"Computing the {cls!r} between {a!r} and {b!r}...")

        if a.uuid != b.uuid:
            raise ValueError(
                f"To compute differences, both arguments must have same identifier. Obtained: {a.uuid!r} vs {b.uuid!r}"
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
    def from_entity(cls, instance: Entity, action: Action = Action.CREATE) -> Delta:
        """Build an ``Delta`` from a ``Entity`` (considering all fields as differences).

        :param instance: A ``Entity`` instance.
        :param action: The action that generates the delta.
        :return: An ``Delta`` instance.
        """
        if action != Action.DELETE:
            fields_diff = FieldDiffContainer.from_model(
                instance, ignore={"uuid", "version", "created_at", "updated_at"}
            )
        else:
            fields_diff = FieldDiffContainer.empty()

        return cls(
            uuid=instance.uuid,
            name=instance.classname,
            version=instance.version,
            action=action,
            created_at=instance.updated_at,
            fields_diff=fields_diff,
        )

    def decompose(self) -> list[Delta]:
        """Decompose the ``Delta`` fields into multiple ``Delta`` instances with once Field.

        :return: An list of``Delta`` instances.
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
