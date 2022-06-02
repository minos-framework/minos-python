import unittest
from uuid import (
    uuid4,
)

from minos.common import (
    ComposedDatabaseOperation,
)
from minos.plugins.lmdb import (
    LmdbDatabaseOperation,
    LmdbDatabaseOperationType,
    LmdbSagaExecutionDatabaseOperationFactory,
)
from minos.saga import (
    Saga,
    SagaExecution,
    SagaExecutionDatabaseOperationFactory,
)


def _fn(context):
    pass


class TestLmdbSagaExecutionDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution = SagaExecution.from_definition(Saga().local_step(_fn).commit())

    def test_is_subclass(self):
        self.assertTrue(issubclass(LmdbSagaExecutionDatabaseOperationFactory, SagaExecutionDatabaseOperationFactory))

    def test_build_create(self):
        factory = LmdbSagaExecutionDatabaseOperationFactory()

        operation = factory.build_create()
        self.assertIsInstance(operation, ComposedDatabaseOperation)
        self.assertEqual(0, len(operation.operations))

    def test_build_store(self):
        factory = LmdbSagaExecutionDatabaseOperationFactory()

        operation = factory.build_store(**self.execution.raw)
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
