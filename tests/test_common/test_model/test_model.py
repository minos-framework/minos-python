from typing import Optional

import pytest

from minos.common import MinosModelException, MinosModelAttributeException, ModelField
from tests.modelClasses import CustomerAggregate, CustomerFailListAggregate, CustomerFailDictAggregate, UserAggregate
import unittest


class TestMinosModel(unittest.TestCase):

    def test_aggregate_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = 1234
        aggregate.name = "John"
        aggregate.surname = "Doe"
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("Doe", aggregate.surname)
        self.assertEqual("John", aggregate.name)

    def test_aggregate_int_as_string_type_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = "1234"
        aggregate.name = "John"
        self.assertEqual(1234, aggregate.id)
        self.assertEqual("John", aggregate.name)

    def test_aggregate_wrong_int_type_setter(self):
        aggregate = CustomerAggregate()
        with pytest.raises(MinosModelAttributeException):
            aggregate.id = "1234S"

    def test_aggregate_string_type_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        aggregate.name = "John"
        self.assertEqual("John", aggregate.name)

    def test_aggregate_wrong_string_type_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        with pytest.raises(MinosModelAttributeException):
            aggregate.name = 456

    def test_aggregate_bool_type_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        aggregate.name = "John"
        aggregate.is_admin = True
        self.assertTrue(aggregate.is_admin)

    def test_aggregate_wrong_bool_type_setter(self):
        aggregate = CustomerAggregate()
        aggregate.id = 123
        aggregate.name = "John"
        with pytest.raises(MinosModelAttributeException):
            aggregate.is_admin = "True"

    def test_aggregate_is_freezed_class(self):
        aggregate = CustomerAggregate()
        with pytest.raises(MinosModelException):
            aggregate.address = "str kennedy"

    def test_aggregate_list_class_attribute(self):
        aggregate = CustomerAggregate()
        aggregate.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], aggregate.lists)

    def test_aggregate_list_wrong_attribute_type(self):
        aggregate = CustomerAggregate()
        with pytest.raises(MinosModelAttributeException):
            aggregate.lists = [1, "hola", 8, 6]

    def test_aggregate_fail_list_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailListAggregate()

    def test_aggregate_fail_dict_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailDictAggregate()

    def test_fields(self):
        fields = {
            'id': ModelField("id", Optional[int], None), 'username': ModelField("username", Optional[str], None)
        }
        model = UserAggregate()
        self.assertEqual(fields, model.fields)

    def test_equal(self):
        a, b = UserAggregate(), UserAggregate()
        a.id = 123
        b.id = 123
        self.assertEqual(a, b)

    def test_not_equal(self):
        a, b = UserAggregate(), UserAggregate()
        a.id = 123
        b.id = 456
        self.assertNotEqual(a, b)

    def test_iter(self):
        user = UserAggregate()
        user.id = 123

        expected = {
            'id': ModelField("id", Optional[int], 123),
            'username': ModelField("username", Optional[str], None)
        }
        self.assertEqual(expected, dict(user))

    def test_hash(self):
        user = UserAggregate()
        user.id = 123

        expected = hash(
            (
                ('username', ModelField("username", Optional[str], None)),
                ('id', ModelField("id", Optional[int], 123)),
            )
        )
        self.assertEqual(expected, hash(user))


if __name__ == '__main__':
    unittest.main()
