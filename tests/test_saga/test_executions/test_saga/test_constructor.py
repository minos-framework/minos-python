import unittest
import warnings
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
    SagaNotCommittedException,
    SagaStatus,
)
from tests.utils import (
    ADD_ORDER,
    Foo,
)


class TestSagaExecutionConstructor(unittest.IsolatedAsyncioTestCase):
    def test_from_saga(self):
        with patch("minos.saga.SagaExecution.from_definition", return_value=56) as mock:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", DeprecationWarning)
                execution = SagaExecution.from_saga(ADD_ORDER)

        self.assertEqual(1, mock.call_count)
        self.assertEqual(56, execution)

    def test_from_definition(self):

        execution = SagaExecution.from_definition(ADD_ORDER)
        self.assertIsInstance(execution, SagaExecution)

        self.assertEqual(ADD_ORDER, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(SagaContext(), execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)

    def test_from_saga_raises(self):
        with self.assertRaises(SagaNotCommittedException):
            SagaExecution.from_definition(Saga())

    def test_from_saga_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")
        execution = SagaExecution.from_definition(ADD_ORDER, context=context)
        self.assertEqual(ADD_ORDER, execution.definition)
        self.assertIsInstance(execution.uuid, UUID)
        self.assertEqual(context, execution.context)
        self.assertEqual(SagaStatus.Created, execution.status)
        self.assertFalse(execution.already_rollback)
        self.assertIsNone(execution.paused_step)


if __name__ == "__main__":
    unittest.main()
