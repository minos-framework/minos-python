import unittest
from unittest.mock import (
    AsyncMock,
)

from minos.saga import (
    LocalSagaStep,
    LocalSagaStepExecution,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaRollbackExecutionStepException,
    SagaStepStatus,
)
from tests.utils import (
    MinosTestCase,
    create_payment,
    create_payment_raises,
    delete_payment,
)


class TestLocalSagaStepExecution(MinosTestCase):
    async def test_execute(self):
        step = LocalSagaStep(create_payment)
        execution = LocalSagaStepExecution(step)

        observed = await execution.execute(SagaContext())

        self.assertEqual(SagaContext(payment="payment"), observed)
        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_execute_raises(self):
        step = LocalSagaStep(create_payment_raises).on_failure(delete_payment)
        context = SagaContext()
        execution = LocalSagaStepExecution(step)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context)

        self.assertEqual(SagaContext(), context)
        self.assertEqual(SagaStepStatus.ErroredOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_execute_already(self):
        mock = AsyncMock()
        step = LocalSagaStep(mock)
        execution = LocalSagaStepExecution(step)

        await execution.execute(SagaContext())
        self.assertEqual(1, mock.call_count)

        mock.reset_mock()
        await execution.execute(SagaContext())
        self.assertEqual(0, mock.call_count)

    async def test_rollback(self):
        step = LocalSagaStep(create_payment).on_failure(delete_payment)
        execution = LocalSagaStepExecution(step)

        await execution.execute(SagaContext())

        observed = await execution.rollback(SagaContext(payment="payment"))

        self.assertEqual(SagaContext(payment=None), observed)

        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(SagaContext(payment="payment"))

    async def test_rollback_raises(self):
        step = LocalSagaStep(create_payment).on_failure(create_payment_raises)
        context = SagaContext()
        execution = LocalSagaStepExecution(step)

        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(context)


if __name__ == "__main__":
    unittest.main()
