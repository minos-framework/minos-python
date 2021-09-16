import unittest
from collections.abc import (
    Mapping,
)

from minos.common import (
    Model,
)


class TestModel(unittest.TestCase):
    def test_base(self):
        self.assertTrue(issubclass(Model, Mapping))


if __name__ == "__main__":
    unittest.main()
