
from minos.common.model.aggregate import aggregate

@aggregate
class TestAggregate:
    id: int
    name: str
    listes: list[int]
    dicts: dict[int, str]


def test_aggregate_model():
    aggregate = TestAggregate()
    assert aggregate._FIELDS == "bravo"
    assert False == True
