from minos.common import MinosModel


class Aggregate(MinosModel):
    """
    base class derived directly from MinosModel
    """
    id: int


class UserAggregate(Aggregate):
    """
    Class for Inheritance Test
    """
    username: str


class CustomerAggregate(UserAggregate):
    """
    Test a Model Class with List
    """
    name: str
    surname: str
    is_admin: bool
    lists: list[int]


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
