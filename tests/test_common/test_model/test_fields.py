import unittest

from minos.common.model.fields import ModelField
from minos.common.exceptions import MinosModelAttributeException


class TestModelField(unittest.TestCase):

    def test_value_dict(self):
        field = ModelField("test", dict[str, bool], {"foo": True, "bar": False})
        self.assertEqual({"foo": True, "bar": False}, field.value)

    def test_value_setter_dict(self):
        field = ModelField("test", dict[str, bool], {})
        field.value = {"foo": True, "bar": False}
        self.assertEqual({"foo": True, "bar": False}, field.value)

    def test_value_setter_dict_raises(self):
        field = ModelField("test", dict[str, bool], {})
        with self.assertRaises(MinosModelAttributeException):
            field.value = "foo"
        with self.assertRaises(MinosModelAttributeException):
            field.value = {"foo": 1, "bar": 2}
        with self.assertRaises(MinosModelAttributeException):
            field.value = {1: True, 2: False}


if __name__ == '__main__':
    unittest.main()
