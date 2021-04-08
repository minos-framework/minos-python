import pytest

from minos.common.exceptions import MinosModelException, MinosModelAttributeException
from tests.modelClasses import CustomerAggregate, CustomerFailListAggregate


def test_aggregate_setter():
    aggregate = CustomerAggregate()
    aggregate.id = 1234
    aggregate.name = "John"
    aggregate.surname = "Doe"
    assert aggregate.id == 1234
    assert aggregate.surname == "Doe"
    assert aggregate.name == "John"
    assert aggregate.fields == "ciao"


def test_aggregate_freeze_class():
    with pytest.raises(MinosModelException):
        aggregate = CustomerAggregate()
        aggregate.address = "str kennedy"


def test_aggregate_fail_class_structure():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerFailListAggregate()
