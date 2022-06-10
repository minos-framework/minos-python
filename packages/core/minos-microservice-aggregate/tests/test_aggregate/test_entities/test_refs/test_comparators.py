import unittest
from typing import (
    Union,
)

from minos.aggregate import (
    Ref,
    is_ref_subclass,
)
from tests.utils import (
    Car,
)


class TestIsRefSubclass(unittest.TestCase):
    def test_is_optional_true(self):
        self.assertTrue(is_ref_subclass(Ref))
        self.assertTrue(is_ref_subclass(Ref[Car]))
        self.assertTrue(is_ref_subclass(Ref["Car"]))

    def test_is_optional_false(self):
        self.assertFalse(is_ref_subclass(int))
        self.assertFalse(is_ref_subclass(Car))
        self.assertFalse(is_ref_subclass(Union[int, str]))


if __name__ == "__main__":
    unittest.main()
