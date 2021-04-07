from collections import Sequence
from typing import Optional, Union, Dict

from minos.common.model.aggregate import aggregate

class User: ...

@aggregate
class TestAggregate:
    id: Optional[int]
    name: str
    listes: list[int]
    dicts: dict[str, int]
    users: Sequence[User]


def test_aggregate_model():
    aggregate = TestAggregate()
    fields = aggregate._FIELDS
    assert fields[3].get_avro_type() == "bravo"
    assert False == True
