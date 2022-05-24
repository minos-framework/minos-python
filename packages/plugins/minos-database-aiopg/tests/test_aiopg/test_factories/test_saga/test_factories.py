import unittest
from uuid import (
    uuid4,
)

from minos.plugins.aiopg import (
    AiopgDatabaseOperation,
    AiopgSagaExecutionDatabaseOperationFactory,
)
from minos.saga import (
    Saga,
    SagaExecution,
    SagaExecutionDatabaseOperationFactory,
)


def _fn(context):
    pass


class TestAiopgSagaExecutionDatabaseOperationFactory(unittest.TestCase):
    def setUp(self) -> None:
        super().setUp()

        self.execution = SagaExecution.from_definition(Saga().local_step(_fn).commit())

    def test_is_subclass(self):
        self.assertTrue(issubclass(AiopgSagaExecutionDatabaseOperationFactory, SagaExecutionDatabaseOperationFactory))

    def test_build_store(self):
        factory = AiopgSagaExecutionDatabaseOperationFactory()

        operation = factory.build_store(**self.execution.raw)
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
