import unittest
from abc import (
    ABC,
)

from minos.common import (
    MinosRepository,
    MinosSetup,
)


class TestMinosRepository(unittest.IsolatedAsyncioTestCase):
    def test_subclass(self):
        self.assertTrue(issubclass(MinosRepository, (ABC, MinosSetup)))

    def test_abstract(self):
        # noinspection PyUnresolvedReferences
        self.assertEqual({"_submit", "_select", "_offset"}, MinosRepository.__abstractmethods__)


if __name__ == "__main__":
    unittest.main()
