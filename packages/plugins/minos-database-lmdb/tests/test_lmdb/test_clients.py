import unittest

from minos.common import (
    DatabaseOperation,
)
from minos.plugins.lmdb import (
    LmdbDatabaseOperation,
)


class TestLmdbDatabaseClient(unittest.TestCase):
    def test_subclass(self) -> None:
        self.assertTrue(issubclass(LmdbDatabaseOperation, DatabaseOperation))


if __name__ == "__main__":
    unittest.main()
