import pytest

from minos.common import MinosModelException, MinosModelAttributeException
from tests.modelClasses import CustomerAggregate, CustomerFailListAggregate, CustomerFailDictAggregate


def test_aggregate_setter():
    aggregate = CustomerAggregate()
    aggregate.id = 1234
    aggregate.name = "John"
    aggregate.surname = "Doe"
    assert aggregate.id == 1234
    assert aggregate.surname == "Doe"
    assert aggregate.name == "John"


def test_aggregate_int_as_string_type_setter():
    aggregate = CustomerAggregate()
    aggregate.id = "1234"
    aggregate.name = "John"
    assert aggregate.id == 1234
    assert aggregate.name == "John"


def test_aggregate_wrong_int_type_setter():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerAggregate()
        aggregate.id = "1234S"
        aggregate.name = "John"


def test_aggregate_string_type_setter():
    aggregate = CustomerAggregate()
    aggregate.id = 123
    aggregate.name = "John"
    assert aggregate.name == "John"


def test_aggregate_wrong_string_type_setter():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        aggregate.name = 456
        assert aggregate.name == "John"


def test_aggregate_bool_type_setter():
    aggregate = CustomerAggregate()
    aggregate.id = 123
    aggregate.name = "John"
    aggregate.is_admin = True
    assert aggregate.is_admin == True


def test_aggregate_wrong_bool_type_setter():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        aggregate.name = "John"
        aggregate.is_admin = "True"


def test_aggregate_is_freezed_class():
    with pytest.raises(MinosModelException):
        aggregate = CustomerAggregate()
        aggregate.address = "str kennedy"


def test_aggregate_list_class_attribute():
    aggregate = CustomerAggregate()
    aggregate.lists = [1, 5, 8, 6]
    assert aggregate.lists[1] == 5


def test_aggregate_list_wrong_attribute_type():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerAggregate()
        aggregate.lists = [1, "hola", 8, 6]
        assert aggregate.lists[1] == 5


def test_aggregate_fail_list_class_attribute():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerFailListAggregate()


def test_aggregate_fail_dict_class_attribute():
    with pytest.raises(MinosModelAttributeException):
        aggregate = CustomerFailDictAggregate()
