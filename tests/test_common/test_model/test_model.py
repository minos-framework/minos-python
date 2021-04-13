import pytest

from minos.common import MinosModelException, MinosModelAttributeException
from tests.modelClasses import CustomerAggregate, CustomerFailListAggregate, CustomerFailDictAggregate
import unittest


class TestMinosModel(unittest.TestCase):

    def test_constructor_args(self):
        aggregate = CustomerAggregate(1234, "johndoe", "John", "Doe")
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("johndoe", aggregate.username)
        self.assertEqual("John", aggregate.name)
        self.assertEqual("Doe", aggregate.surname)

    def test_constructor_multiple_values(self):
        with self.assertRaises(TypeError):
            CustomerAggregate(1234, id=1234)
        with self.assertRaises(TypeError):
            CustomerAggregate(None, id=1234)

    def test_constructor_kwargs(self):
        aggregate = CustomerAggregate(id=1234, username="johndoe", name="John", surname="Doe")
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("johndoe", aggregate.username)
        self.assertEqual("Doe", aggregate.surname)
        self.assertEqual("John", aggregate.name)

    def test_aggregate_setter(self):
        aggregate = CustomerAggregate(1234)
        aggregate.name = "John"
        aggregate.surname = "Doe"
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("Doe", aggregate.surname)
        self.assertEqual("John", aggregate.name)

    def test_aggregate_int_as_string_type_setter(self):
        aggregate = CustomerAggregate("1234")
        aggregate.name = "John"
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("John", aggregate.name)

    def test_aggregate_wrong_int_type_setter(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerAggregate("1234S")

    def test_aggregate_string_type_setter(self):
        aggregate = CustomerAggregate(123)
        aggregate.name = "John"
        self.assertEqual("John", aggregate.name)

    def test_aggregate_wrong_string_type_setter(self):
        aggregate = CustomerAggregate(123)
        with pytest.raises(MinosModelAttributeException):
            aggregate.name = 456

    def test_aggregate_bool_type_setter(self):
        aggregate = CustomerAggregate(123)
        aggregate.name = "John"
        aggregate.is_admin = True
        self.assertTrue(aggregate.is_admin)

    def test_aggregate_wrong_bool_type_setter(self):
        aggregate = CustomerAggregate(123)
        aggregate.name = "John"
        with pytest.raises(MinosModelAttributeException):
            aggregate.is_admin = "True"

    def test_aggregate_empty_mandatory_field(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerAggregate()

    def test_aggregate_is_freezed_class(self):
        aggregate = CustomerAggregate(123)
        with pytest.raises(MinosModelException):
            aggregate.address = "str kennedy"

    def test_aggregate_list_class_attribute(self):
        aggregate = CustomerAggregate(123)
        aggregate.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], aggregate.lists)

    def test_aggregate_list_wrong_attribute_type(self):
        aggregate = CustomerAggregate(123)
        with pytest.raises(MinosModelAttributeException):
            aggregate.lists = [1, "hola", 8, 6]

    def test_aggregate_fail_list_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailListAggregate(123)

    def test_aggregate_fail_dict_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailDictAggregate(123)


if __name__ == '__main__':
    unittest.main()
