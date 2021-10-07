import unittest
from unittest.mock import (
    MagicMock,
)

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    MinosSagaExecutionAlreadyExecutedException,
    MinosSagaFailedCommitCallbackException,
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    commit_callback,
    commit_callback_raises,
    fake_reply,
    handle_order_success,
    handle_ticket_success,
    handle_ticket_success_raises,
    send_create_order,
    send_create_ticket,
    send_delete_order,
    send_delete_ticket,
)


class TestSagaExecution(unittest.IsolatedAsyncioTestCase):
    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()

        self.publish_mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = self.publish_mock

    async def test_execute(self):
        saga = (
            Saga()
            .step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit()
        )
        execution = SagaExecution.from_saga(saga)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("ticket"))
        context = await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket")), context)
        self.assertEqual(2, self.publish_mock.call_count)
        with self.assertRaises(MinosSagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_failure(self):
        saga = (
            Saga()
            .step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .step(send_create_ticket)
            .on_success(handle_ticket_success_raises)
            .on_failure(send_delete_ticket)
            .commit()
        )
        execution = SagaExecution.from_saga(saga)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        self.publish_mock.reset_mock()
        reply = fake_reply(Foo("ticket"))
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(2, self.publish_mock.call_count)

        reply = fake_reply(Foo("fixed failure!"), execution.uuid)
        await execution.execute(reply=reply)

        with self.assertRaises(MinosSagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_commit(self):
        saga = (
            Saga()
            .step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit(commit_callback)
        )
        execution = SagaExecution.from_saga(saga)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("ticket"))
        context = await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order=Foo("order"), ticket=Foo("ticket"), status="Finished!"), context)
        self.assertEqual(2, self.publish_mock.call_count)

    async def test_execute_commit_raises(self):
        saga = (
            Saga()
            .step(send_create_order)
            .on_success(handle_order_success)
            .on_failure(send_delete_order)
            .step(send_create_ticket)
            .on_success(handle_ticket_success)
            .commit(commit_callback_raises)
        )
        execution = SagaExecution.from_saga(saga)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order1"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order2"))
        with self.assertRaises(MinosSagaFailedCommitCallbackException):
            await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(3, self.publish_mock.call_count)

    async def test_rollback(self):
        saga = Saga().step(send_create_order).on_success(handle_order_success).on_failure(send_delete_order).commit()
        execution = SagaExecution.from_saga(saga)
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        reply = fake_reply(Foo("order1"))
        await execution.execute(reply=reply, broker=self.broker)

        self.publish_mock.reset_mock()
        await execution.rollback(broker=self.broker)
        self.assertEqual(1, self.publish_mock.call_count)

        self.publish_mock.reset_mock()
        with self.assertRaises(MinosSagaRollbackExecutionException):
            await execution.rollback(broker=self.broker)
        self.assertEqual(0, self.publish_mock.call_count)

    async def test_rollback_raises(self):
        saga = Saga().step(send_create_order).on_success(handle_order_success).on_failure(send_delete_order).commit()
        execution = SagaExecution.from_saga(saga)
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        reply = fake_reply(Foo("order1"))
        await execution.execute(reply=reply, broker=self.broker)

        async def _fn(*args, **kwargs):
            raise ValueError("This is an exception")

        self.publish_mock.side_effect = _fn
        self.publish_mock.reset_mock()

        with self.assertRaises(MinosSagaRollbackExecutionException):
            await execution.rollback(broker=self.broker)


if __name__ == "__main__":
    unittest.main()
