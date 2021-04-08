from minos.common.model.aggregate import MinosModel


class Aggregate(MinosModel):
    """
    base class derived directly from MinosModel
    """
    id: int


class UserAggregate(Aggregate):
    username: str


class CustomerAggregate(UserAggregate):
    name: str
    surname: str
    listes: list[int]
    # dicts: dict[str, int]
    # users: Sequence[User]


class CustomerFailListAggregate(UserAggregate):
    name: str
    surname: str
    listes_failed: list
