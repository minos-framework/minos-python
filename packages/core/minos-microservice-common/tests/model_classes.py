from enum import (
    Enum,
)
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
    MissingSentinel,
)


class Foo(DeclarativeModel):
    """For testing purposes"""

    text: str


class Bar(DeclarativeModel):
    """For testing purposes"""

    first: Foo
    second: Foo


class FooBar(DeclarativeModel):
    """For testing purposes"""

    identifier: UUID


class Base(DeclarativeModel):
    """
    base class derived directly from DeclarativeModel
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


class ShoppingList(DeclarativeModel):
    """Class to test ``DeclarativeModel`` composition."""

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
    """Class to test ``DeclarativeModel`` recursive type validation."""

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


class CustomerFailList(DeclarativeModel):
    """
    Test a Model Class with a List wrong formatted
    """

    friends: list


class CustomerFailDict(DeclarativeModel):
    """
    Test a Model Class with a Dictionary wrong formatted
    """

    friends: dict


T = TypeVar("T", str, int)


class GenericUser(DeclarativeModel, Generic[T]):
    """For testing purposes."""

    username: T


class ReservedWordUser(DeclarativeModel, Generic[T]):
    """For testing purposes."""

    items: str


class Auth(DeclarativeModel):
    """For testing purposes."""

    user: GenericUser[str]


class Owner(DeclarativeModel):
    """For testing purposes."""

    name: str
    surname: str
    age: Optional[int]


class Car(DeclarativeModel):
    """For testing purposes."""

    doors: int
    color: str
    owner: Optional[list[Owner]]


class Status(str, Enum):
    """For testing purposes."""

    PENDING = "pending"
    SUCCESS = "success"
    ERROR = "error"


class TextNumber(DeclarativeModel):
    """For testing purposes."""

    text: str
    number: int

    def __init__(self, *args, text: Optional[str] = None, **kwargs):
        if text is None:
            text = "foo"
        super().__init__(text, *args, **kwargs)
