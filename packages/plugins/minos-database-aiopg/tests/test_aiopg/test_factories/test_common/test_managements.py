import unittest

from minos.common import (
    ManagementDatabaseOperationFactory,
)
from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgManagementDatabaseOperationFactory,
)


class TestAiopgManagementDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        self.factory = AiopgManagementDatabaseOperationFactory()

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgManagementDatabaseOperationFactory, ManagementDatabaseOperationFactory))

    def test_build_create(self):
        operation = self.factory.build_create("foo")
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_delete(self):
        operation = self.factory.build_delete("foo")
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
