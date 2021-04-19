"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
from typing import (
    Optional,
)

from minos.common import (
    MinosModel,
    MissingSentinel,
    ModelRef,
)


class Base(MinosModel):
    """
    base class derived directly from MinosModel
    """

    id: int

    @staticmethod
    def validate_id(value: int) -> bool:
        """Validate non negative ids."""
        return value >= 0


class User(Base):
    """
    Class for Inheritance Test
    """

    username: Optional[str]

    @staticmethod
    def parse_username(value: Optional[str]) -> Optional[str]:
        """Parse username into a cleaner format.

        :param value: username to be parsed.
        :return: An string object.
        """
        if value is None or value is MissingSentinel:
            return None
        return value.lower()

    def validate_username(self, value: str) -> bool:
        """Validate that the username is ``None`` or a single word."""
        if self.id == 0 and value != "admin":
            return False
        return not value.count(" ")


class ShoppingList(MinosModel):
    """Class to test ``MinosModel`` composition."""

    user: Optional[ModelRef[User]]
    cost: float

    def parse_cost(self, value: Optional[str]) -> float:
        """Parse a number encoded as string with a semicolon as decimal separator.

        :param value: cost to be parsed.
        :return: A float value.
        """
        if self.user is not None:
            if self.user.username == "admin":
                return 0.0

        if value is None or value is MissingSentinel:
            return float("inf")
        if isinstance(value, float):
            return value
        return float(value.replace(".", "").replace(",", "."))


class Analytics(Base):
    """Class to test ``MinosModel`` recursive type validation."""

    orders: dict[str, list[ModelRef[ShoppingList]]]


class Customer(User):
    """
    Test a Model Class with List
    """

    name: Optional[str]
    surname: Optional[str]
    is_admin: Optional[bool]
    lists: Optional[list[int]]

    @staticmethod
    def parse_name(name: str) -> Optional[str]:
        """Parse name into a cleaner format.

        :param name: name to be parsed.
        :return: An string object.
        """
        if name is None or name is MissingSentinel:
            return None
        return name.title()


class CustomerDict(User):
    """
    Test a Model Class with Dictionary
    """

    name: str
    surname: str
    friends: dict[str, str]


class CustomerFailList(User):
    """
    Test a Model Class with a List wrong formatted
    """

    name: Optional[str]
    surname: Optional[str]
    listes_failed: Optional[list]


class CustomerFailDict(User):
    """
    Test a Model Class with a Dictionary wrong formatted
    """

    name: Optional[str]
    surname: Optional[str]
    friends: dict
