import unittest
from typing import Optional, Union, List

from minos.common import ModelField, MinosModelAttributeException


class TestModelField(unittest.TestCase):

    def test_name(self):
        field = ModelField("test", int, 3)
        self.assertEqual("test", field.name)

    def test_type(self):
        field = ModelField("test", int, 3)
        self.assertEqual({"origin": int}, field.type)

    def test_value(self):
        field = ModelField("test", int, 3)
        self.assertEqual(3, field.value)

    def test_value_setter(self):
        field = ModelField("test", int, 3)
        field.value = 3
        self.assertEqual(3, field.value)

    def test_value_setter_raises(self):
        field = ModelField("test", int, 3)
        with self.assertRaises(MinosModelAttributeException):
            field.value = None

    def test_optional_type(self):
        field = ModelField("test", Optional[int], None)
        self.assertEqual({"origin": Union, "alternatives": (int, type(None),)}, field.type)

    def test_value_setter_optional_int(self):
        field = ModelField("test", Optional[int], 3)
        self.assertEqual(3, field.value)
        field.value = None
        self.assertEqual(None, field.value)
        field.value = 4
        self.assertEqual(4, field.value)

    def test_value_setter_optional_list(self):
        field = ModelField("test", Optional[List[int]], [1, 2, 3])
        self.assertEqual([1, 2, 3], field.value)
        field.value = None
        self.assertEqual(None, field.value)
        field.value = [4]
        self.assertEqual([4], field.value)


if __name__ == '__main__':
    unittest.main()
