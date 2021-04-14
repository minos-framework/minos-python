from typing import Optional

from minos.common import MinosModel, ModelRef


class Base(MinosModel):
    """
    base class derived directly from MinosModel
    """
    id: int


class User(Base):
    """
    Class for Inheritance Test
    """
    username: Optional[str]


class ShoppingList(MinosModel):
    """Class to test ``MinosModel`` composition."""
    user: Optional[ModelRef[User]]
    cost: float

    def parse_cost(self, value: Optional[str]) -> float:
        """Parse a number encoded as string with a semicolon as decimal separator.

        :param value: cost to be parsed.
        :return: A float value.
        """
        if value is None:
            return 0.0
        return float(value.replace(".", "").replace(",", "."))


class Customer(User):
    """
    Test a Model Class with List
    """
    name: Optional[str]
    surname: Optional[str]
    is_admin: Optional[bool]
    lists: Optional[list[int]]

    def parse_name(self, name: str) -> str:
        """Parse name into a cleaner format.

        :param name: name to be parsed.
        :return: An string object.
        """
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
    name: str
    surname: str
    listes_failed: list


class CustomerFailDict(User):
    """
    Test a Model Class with a Dictionary wrong formatted
    """
    name: str
    surname: str
    friends: dict
