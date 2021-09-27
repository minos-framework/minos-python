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
from tests.callbacks import (
    commit_callback,
    commit_callback_raises,
    create_order_callback,
    create_ticket_callback,
    delete_order_callback,
    shipping_callback,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    foo_fn_raises,
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
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1")
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply("order2")
            .step()
            .invoke_participant("Shopping", shipping_callback)
            .with_compensation("BlockOrder", shipping_callback)
            .commit()
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
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply()
        context = await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order1=Foo("order1"), order2=Foo("order2")), context)
        self.assertEqual(3, self.publish_mock.call_count)
        with self.assertRaises(MinosSagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_failure(self):
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1")
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order2", foo_fn_raises)
            .commit()
        )
        execution = SagaExecution.from_saga(saga)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order1"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        self.publish_mock.reset_mock()
        reply = fake_reply(Foo("order2"))
        with self.assertRaises(MinosSagaFailedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(2, self.publish_mock.call_count)

        reply = fake_reply(Foo("order2"), execution.uuid)
        await execution.execute(reply=reply)

        with self.assertRaises(MinosSagaExecutionAlreadyExecutedException):
            await execution.execute()

    async def test_execute_commit(self):
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1")
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply("order2")
            .step()
            .invoke_participant("Shopping", shipping_callback)
            .with_compensation("BlockOrder", shipping_callback)
            .commit(commit_callback)
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
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply()
        context = await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order1=Foo("order1"), order2=Foo("order2"), status="Finished!"), context)
        self.assertEqual(3, self.publish_mock.call_count)

    async def test_execute_commit_raises(self):
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1")
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply("order2")
            .step()
            .invoke_participant("Shopping", shipping_callback)
            .with_compensation("BlockOrder", shipping_callback)
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
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply()
        with self.assertRaises(MinosSagaFailedCommitCallbackException):
            await execution.execute(reply=reply, broker=self.broker)

        self.assertEqual(SagaStatus.Errored, execution.status)
        self.assertEqual(5, self.publish_mock.call_count)

    async def test_rollback(self):
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1", lambda order: order)
            .commit()
        )
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
        saga = (
            Saga()
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1", lambda order: order)
            .commit()
        )
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
