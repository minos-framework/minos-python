import pytest

from minos.common import MinosModelException, MinosModelAttributeException
from tests.modelClasses import Customer, CustomerFailList, CustomerFailDict, ShoppingList, User
import unittest


class TestMinosModel(unittest.TestCase):

    def test_constructor_args(self):
        model = Customer(1234, "johndoe", "John", "Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("John", model.name)
        self.assertEqual("Doe", model.surname)

    def test_constructor_multiple_values(self):
        with self.assertRaises(TypeError):
            Customer(1234, id=1234)
        with self.assertRaises(TypeError):
            Customer(None, id=1234)

    def test_constructor_kwargs(self):
        model = Customer(id=1234, username="johndoe", name="John", surname="Doe")
        self.assertEqual(1234, model.id)
        self.assertEqual("johndoe", model.username)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_aggregate_setter(self):
        model = Customer(1234)
        model.name = "John"
        model.surname = "Doe"
        self.assertEqual(1234, model.id)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_aggregate_int_as_string_type_setter(self):
        model = Customer("1234")
        model.name = "John"
        self.assertEqual(1234, model.id)
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_int_type_setter(self):
        with pytest.raises(MinosModelAttributeException):
            Customer("1234S")

    def test_aggregate_string_type_setter(self):
        model = Customer(123)
        model.name = "John"
        self.assertEqual("John", model.name)

    def test_aggregate_wrong_string_type_setter(self):
        model = Customer(123)
        with pytest.raises(MinosModelAttributeException):
            model.name = 456

    def test_aggregate_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        model.is_admin = True
        self.assertTrue(model.is_admin)

    def test_aggregate_wrong_bool_type_setter(self):
        model = Customer(123)
        model.name = "John"
        with pytest.raises(MinosModelAttributeException):
            model.is_admin = "True"

    def test_aggregate_empty_mandatory_field(self):
        with pytest.raises(MinosModelAttributeException):
            Customer()

    def test_model_is_freezed_class(self):
        model = Customer(123)
        with pytest.raises(MinosModelException):
            model.address = "str kennedy"

    def test_model_list_class_attribute(self):
        model = Customer(123)
        model.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], model.lists)

    def test_model_list_wrong_attribute_type(self):
        model = Customer(123)
        with pytest.raises(MinosModelAttributeException):
            model.lists = [1, "hola", 8, 6]

    def test_model_ref(self):
        shopping_list = ShoppingList()
        user = User(1234)
        shopping_list.user = user
        self.assertEqual(user, shopping_list.user)

    def test_model_ref_raises(self):
        shopping_list = ShoppingList()
        with self.assertRaises(MinosModelAttributeException):
            shopping_list.user = "foo"

    def test_model_fail_list_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailList(123)

    def test_model_fail_dict_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailDict(123)


if __name__ == '__main__':
    unittest.main()
