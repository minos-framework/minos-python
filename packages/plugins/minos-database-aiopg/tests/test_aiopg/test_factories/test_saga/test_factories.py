import unittest
from uuid import (
    uuid4,
)

from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgSagaExecutionDatabaseOperationFactory,
)
from minos.saga import (
    SagaExecutionDatabaseOperationFactory,
)


class TestAiopgSagaExecutionDatabaseOperationFactory(unittest.TestCase):
    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgSagaExecutionDatabaseOperationFactory, SagaExecutionDatabaseOperationFactory))

    @unittest.skip
    def test_build_store(self):
        factory = AiopgSagaExecutionDatabaseOperationFactory()

        operation = factory.build_store(uuid4(), foo="bar")
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_load(self):
        factory = AiopgSagaExecutionDatabaseOperationFactory()

        operation = factory.build_load(uuid4())
        self.assertIsInstance(operation, AiopgDatabaseOperation)

    def test_build_delete(self):
        factory = AiopgSagaExecutionDatabaseOperationFactory()

        operation = factory.build_delete(uuid4())
        self.assertIsInstance(operation, AiopgDatabaseOperation)


if __name__ == "__main__":
    unittest.main()
