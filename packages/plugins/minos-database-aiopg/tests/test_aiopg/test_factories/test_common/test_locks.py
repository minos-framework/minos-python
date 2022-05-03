import unittest

from minos.common import (
    LockDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgLockDatabaseOperationFactory,
)


class TestAiopgLockDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgLockDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgLockDatabaseOperationFactory, LockDatabaseOperationFactory))

    def test_build_acquire(self):
        operation = self.factory.build_acquire(56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_release(self):
        operation = self.factory.build_release(56)
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
