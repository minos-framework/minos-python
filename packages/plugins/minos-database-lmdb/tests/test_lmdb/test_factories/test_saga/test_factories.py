import unittest
from uuid import (
    uuid4,
)

from minos.plugins.lmdb import (
    LmdbDatabaseOperation,
    LmdbDatabaseOperationType,
    LmdbSagaExecutionDatabaseOperationFactory,
)
from minos.saga import (
    SagaExecutionDatabaseOperationFactory,
)


class TestLmdbSagaExecutionDatabaseOperationFactory(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(LmdbSagaExecutionDatabaseOperationFactory, SagaExecutionDatabaseOperationFactory))

    def test_build_store(self):
        factory = LmdbSagaExecutionDatabaseOperationFactory()

        operation = factory.build_store(uuid4(), foo="bar")
        self.assertIsInstance(operation, LmdbDatabaseOperation)
        self.assertEqual(LmdbDatabaseOperationType.CREATE, operation.type_)

    def test_build_load(self):
        factory = LmdbSagaExecutionDatabaseOperationFactory()

        operation = factory.build_load(uuid4())
        self.assertIsInstance(operation, LmdbDatabaseOperation)
        self.assertEqual(LmdbDatabaseOperationType.READ, operation.type_)

    def test_build_delete(self):
        factory = LmdbSagaExecutionDatabaseOperationFactory()

        operation = factory.build_delete(uuid4())
        self.assertIsInstance(operation, LmdbDatabaseOperation)
        self.assertEqual(LmdbDatabaseOperationType.DELETE, operation.type_)


if __name__ == "__main__":
    unittest.main()
