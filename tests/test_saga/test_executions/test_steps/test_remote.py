import unittest
from unittest.mock import (
    AsyncMock,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    RemoteSagaStep,
    RemoteSagaStepExecution,
    SagaContext,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaResponse,
    SagaResponseStatus,
    SagaRollbackExecutionStepException,
    SagaStepStatus,
)
from tests.utils import (
    Foo,
    NaiveBroker,
    handle_ticket_error,
    handle_ticket_error_raises,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_ticket,
    send_create_ticket_raises,
    send_delete_ticket,
)


class TestRemoteSagaStepExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.broker = NaiveBroker()
        self.execute_kwargs = {
            "execution_uuid": uuid4(),
            "broker": self.broker,
            "user": uuid4(),
        }

        self.publish_mock = AsyncMock()
        self.broker.send = self.publish_mock

    async def test_on_execute(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.assertEqual(1, self.publish_mock.call_count)
        self.assertEqual(SagaStepStatus.PausedByOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_execute_raises(self):
        step = RemoteSagaStep(send_create_ticket_raises).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.assertEqual(0, self.publish_mock.call_count)
        self.assertEqual(SagaStepStatus.ErroredOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_errored_response(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(status=SagaResponseStatus.SYSTEM_ERROR)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredByOnExecute, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_success(self):
        step = RemoteSagaStep(send_create_ticket).on_success(handle_ticket_success)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.SUCCESS)

        await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)

    async def test_on_success_not_defined(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.SUCCESS)

        await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_success_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_success(handle_ticket_success_raises)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.SUCCESS)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnSuccess, execution.status)
        self.assertEqual(1, rollback_mock.call_count)

    async def test_on_error(self):
        step = RemoteSagaStep(send_create_ticket).on_error(handle_ticket_error)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.ERROR)

        await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_error_not_defined(self):
        step = RemoteSagaStep(send_create_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.ERROR)

        await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.Finished, execution.status)
        self.assertEqual(0, rollback_mock.call_count)

    async def test_on_error_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_error(handle_ticket_error_raises)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step, status=SagaStepStatus.PausedByOnExecute)
        rollback_mock = AsyncMock()
        execution.rollback = rollback_mock
        response = SagaResponse(Foo("foo"), SagaResponseStatus.ERROR)

        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(context, response, **self.execute_kwargs)

        self.assertEqual(SagaStepStatus.ErroredOnError, execution.status)
        self.assertEqual(1, rollback_mock.call_count)

    async def test_rollback(self):
        step = RemoteSagaStep(send_create_ticket).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(context, **self.execute_kwargs)

        self.publish_mock.reset_mock()
        await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)
        self.assertEqual(0, self.publish_mock.call_count)

    async def test_rollback_raises(self):
        step = RemoteSagaStep(send_create_ticket).on_failure(send_delete_ticket)
        context = SagaContext()
        execution = RemoteSagaStepExecution(step)

        with self.assertRaises(SagaRollbackExecutionStepException):
            await execution.rollback(context, **self.execute_kwargs)


if __name__ == "__main__":
    unittest.main()
