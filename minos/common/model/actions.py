"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from __future__ import (
    annotations,
)

from enum import (
    Enum,
)

from ..exceptions import (
    MinosModelException,
)


class Action(str, Enum):
    """Enum class that describes the actions."""

    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"

    @classmethod
    def value_of(cls, value: str) -> Action:
        """Get the action based on its text representation."""
        for item in cls.__members__.values():
            if item.value == value:
                return item
        raise MinosModelException(f"The given value does not match with any enum items. Obtained {value}")

    @property
    def is_create(self) -> bool:
        """Check if the action is create.

        :return: A boolean value.
        """
        return self is Action.CREATE

    @property
    def is_update(self) -> bool:
        """Check if the action is create.

        :return: A boolean value.
        """
        return self is Action.UPDATE

    @property
    def is_delete(self) -> bool:
        """Check if the action is create.

        :return: A boolean value.
        """
        return self is Action.DELETE
