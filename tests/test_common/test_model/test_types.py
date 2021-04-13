import unittest
from typing import Generic

from minos.common import ModelRef, CUSTOM_TYPES


class TestModelRef(unittest.TestCase):

    def test_subclass(self):
        # noinspection PyTypeHints
        self.assertTrue(issubclass(ModelRef, Generic))

    def test_repr(self):
        ref = ModelRef()
        self.assertEqual("ModelRef()", repr(ref))


class TestTypesModule(unittest.TestCase):

    def test_custom_types(self):
        self.assertEqual(("Fixed", "Enum", "Decimal", "ModelRef",), CUSTOM_TYPES)


if __name__ == '__main__':
    unittest.main()
