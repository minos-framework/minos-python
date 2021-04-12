import unittest

from minos.common import ModelField


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


if __name__ == '__main__':
    unittest.main()
