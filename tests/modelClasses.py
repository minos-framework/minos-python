from typing import Optional

from minos.common import MinosModel


class Aggregate(MinosModel):
    """
    base class derived directly from MinosModel
    """
    id: Optional[int]


class UserAggregate(Aggregate):
    """
    Class for Inheritance Test
    """
    username: Optional[str]


class CustomerAggregate(UserAggregate):
    """
    Test a Model Class with List
    """
    name: Optional[str]
    surname: Optional[str]
    is_admin: Optional[bool]
    lists: Optional[list[int]]


class CustomerDictAggregate(UserAggregate):
    """
    Test a Model Class with Dictionary
    """
    name: str
    surname: str
    friends: dict[str, str]


class CustomerFailListAggregate(UserAggregate):
    """
    Test a Model Class with a List wrong formatted
    """
    name: str
    surname: str
    listes_failed: list


class CustomerFailDictAggregate(UserAggregate):
    """
    Test a Model Class with a Dictionary wrong formatted
    """
    name: str
    surname: str
    friends: dict
