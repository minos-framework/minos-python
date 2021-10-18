import unittest
from uuid import (
    UUID,
)

from minos.saga import (
    EmptySagaException,
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)
from tests.utils import (
    ADD_ORDER,
    Foo,
)


class TestSagaExecutionConstructor(unittest.IsolatedAsyncioTestCase):
    def test_from_saga(self):
        execution = SagaExecution.from_saga(ADD_ORDER)
        self.assertEqual(ADD_ORDER, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(SagaContext(), execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)

    def test_from_saga_raises(self):
        with self.assertRaises(EmptySagaException):
            SagaExecution.from_saga(Saga())

    def test_from_saga_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")
        execution = SagaExecution.from_saga(ADD_ORDER, context=context)
        self.assertEqual(ADD_ORDER, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(context, execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)


if __name__ == "__main__":
    unittest.main()
