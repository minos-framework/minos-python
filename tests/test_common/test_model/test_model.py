import pytest

from minos.common import MinosModelException, MinosModelAttributeException
from tests.modelClasses import Customer, CustomerFailList, CustomerFailDict
import unittest


class TestMinosModel(unittest.TestCase):

    def test_model_setter(self):
        model = Customer()
        model.id = 1234
        model.name = "John"
        model.surname = "Doe"
        self.assertEqual(1234, model.id)
        self.assertEqual("Doe", model.surname)
        self.assertEqual("John", model.name)

    def test_model_int_as_string_type_setter(self):
        model = Customer()
        model.id = "1234"
        model.name = "John"
        self.assertEqual(1234, model.id)
        self.assertEqual("John", model.name)

    def test_model_wrong_int_type_setter(self):
        model = Customer()
        with pytest.raises(MinosModelAttributeException):
            model.id = "1234S"

    def test_model_string_type_setter(self):
        model = Customer()
        model.id = 123
        model.name = "John"
        self.assertEqual("John", model.name)

    def test_model_wrong_string_type_setter(self):
        model = Customer()
        model.id = 123
        with pytest.raises(MinosModelAttributeException):
            model.name = 456

    def test_model_bool_type_setter(self):
        model = Customer()
        model.id = 123
        model.name = "John"
        model.is_admin = True
        self.assertTrue(model.is_admin)

    def test_model_wrong_bool_type_setter(self):
        model = Customer()
        model.id = 123
        model.name = "John"
        with pytest.raises(MinosModelAttributeException):
            model.is_admin = "True"

    def test_model_is_freezed_class(self):
        model = Customer()
        with pytest.raises(MinosModelException):
            model.address = "str kennedy"

    def test_model_list_class_attribute(self):
        model = Customer()
        model.lists = [1, 5, 8, 6]

        self.assertEqual([1, 5, 8, 6], model.lists)

    def test_model_list_wrong_attribute_type(self):
        model = Customer()
        with pytest.raises(MinosModelAttributeException):
            model.lists = [1, "hola", 8, 6]

    def test_model_fail_list_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailList()

    def test_model_fail_dict_class_attribute(self):
        with pytest.raises(MinosModelAttributeException):
            CustomerFailDict()


if __name__ == '__main__':
    unittest.main()
