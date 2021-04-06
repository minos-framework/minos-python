from dataclasses import dataclass, field
import pytest
from minos.common.logs import log
from minos.common.model.aggregate import MinosBaseAggregate, aggregate
import typing as t

@aggregate
class TestAggregate:
    id: int
    name: str
    listes: list[int]
    dicts: dict[int, str]


def test_aggregate_model():
    aggregate = TestAggregate()
    assert aggregate._TESTER == "bravo"
    assert False == True
