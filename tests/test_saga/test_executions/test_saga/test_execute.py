import unittest
from unittest.mock import (
    AsyncMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.saga import (
    Saga,
    SagaContext,
    SagaExecution,
    SagaExecutionAlreadyExecutedException,
    SagaFailedCommitCallbackException,
    SagaFailedExecutionStepException,
    SagaPausedExecutionStepException,
    SagaResponse,
    SagaRollbackExecutionException,
    SagaStatus,
)
from tests.utils import (
    Foo,
    MinosTestCase,
    commit_callback,
    commit_callback_raises,
    handle_order_success,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_order,
    send_create_ticket,
    send_delete_order,
    send_delete_ticket,
)


class TestSagaExecution(MinosTestCase):
    def setUp(self) -> None:
        super().setUp()

        self.publish_mock = AsyncMock()
        self.command_broker.send = self.publish_mock

    async def test_execute(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"))
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"))
        context = await execution.execute(response)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket")), context)
        self.assertEqual(2, self.publish_mock.call_count)
        with self.assertRaises(SagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_and_publish(self):
        user = uuid4()

        definition = Saga().remote_step(send_create_order).commit()
        execution = SagaExecution.from_definition(definition, user=user)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()

        self.assertEqual(1, self.publish_mock.call_count)
        args = call(topic="CreateOrder", data=Foo(foo="create_order!"), saga=execution.uuid, user=user,)
        self.assertEqual(args, self.publish_mock.call_args)
        self.assertEqual(SagaStatus.Paused, execution.status)

    async def test_execute_failure(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success_raises)
            .on_failure(send_delete_ticket)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"))
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        self.publish_mock.reset_mock()
        response = SagaResponse(Foo("ticket"))
        with self.assertRaises(SagaFailedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(2, self.publish_mock.call_count)

        response = SagaResponse(Foo("fixed failure!"))
        await execution.execute(response)

        with self.assertRaises(SagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_commit(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit(commit_callback)
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order"))
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("ticket"))
        context = await execution.execute(response)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket"), status="Finished!"), context)
        self.assertEqual(2, self.publish_mock.call_count)

    async def test_execute_commit_raises(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .remote_step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit(commit_callback_raises)
        )
        execution = SagaExecution.from_definition(saga)

        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order1"))
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute(response)
        self.assertEqual(SagaStatus.Paused, execution.status)

        response = SagaResponse(Foo("order2"))
        with self.assertRaises(SagaFailedCommitCallbackException):
            await execution.execute(response)

        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(3, self.publish_mock.call_count)

    async def test_rollback(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        response = SagaResponse(Foo("order1"))
        await execution.execute(response)

        self.publish_mock.reset_mock()
        await execution.rollback()
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(SagaRollbackExecutionException):
            await execution.rollback()
        self.assertEqual(0, self.publish_mock.call_count)

    async def test_rollback_raises(self):
        saga = (
            Saga()
            .remote_step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .commit()
        )
        execution = SagaExecution.from_definition(saga)
        with self.assertRaises(SagaPausedExecutionStepException):
            await execution.execute()
        response = SagaResponse(Foo("order1"))
        await execution.execute(response)

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        self.publish_mock.side_effect = _fn
        self.publish_mock.reset_mock()

        with self.assertRaises(SagaRollbackExecutionException):
            await execution.rollback()


if __name__ == "__main__":
    unittest.main()
