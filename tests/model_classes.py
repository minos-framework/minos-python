from typing import (
    Generic,
    Optional,
    TypeVar,
)
from uuid import (
    UUID,
)

from minos.common import (
    DeclarativeModel,
    MinosModel,
    MissingSentinel,
)


class Foo(MinosModel):
    """For testing purposes"""

    text: str


class Bar(MinosModel):
    """For testing purposes"""

    first: Foo
    second: Foo


class FooBar(MinosModel):
    """For testing purposes"""

    identifier: UUID


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

    user: Optional[User]
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

    orders: dict[str, list[ShoppingList]]


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


class CustomerFailList(MinosModel):
    """
    Test a Model Class with a List wrong formatted
    """

    friends: list


class CustomerFailDict(MinosModel):
    """
    Test a Model Class with a Dictionary wrong formatted
    """

    friends: dict


T = TypeVar("T", str, int)


class GenericUser(DeclarativeModel, Generic[T]):
    """For testing purposes."""

    username: T


class Auth(DeclarativeModel):
    """For testing purposes."""

    user: GenericUser[str]
