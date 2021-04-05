from dataclasses import dataclass, field
import pytest
from minos.common.logs import log
from minos.common.model.aggregate import MinosBaseAggregate
import typing as t

@dataclass
class TestAggregate(MinosBaseAggregate):
    id: int
    name: str = field(default_factory=str)
    listes: list[int] = field(default_factory=list)
    dicts: dict[int, str] = field(default_factory=dict[int, str])


def test_aggregate_model():
    aggregate = TestAggregate(id=123)
    log.debug(f"Testing Aggregate: {aggregate.schema}")
    log.debug(f"Testing Aggregate: {aggregate.listes}")
    log.debug(f"Testing Aggregate: {aggregate.name}")
    log.debug(f"Testing Aggregate: {aggregate.dicts}")
    assert aggregate.schema == []
