"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    MagicMock,
)

from dependency_injector import (
    containers,
    providers,
)

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaRollbackExecutionException,
    Saga,
    SagaContext,
    SagaExecution,
    SagaStatus,
)
from tests.callbacks import (
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

        self.publish_mock = MagicMock(side_effect=self.broker.send_one)
        self.broker.send_one = self.publish_mock

        self.container = containers.DynamicContainer()
        self.container.config = providers.Object(self.config)
        self.container.command_broker = providers.Object(self.broker)

        from minos import (
            saga,
        )

        self.container.wire(modules=[saga])

    def tearDown(self) -> None:
        self.container.unwire()

    async def test_execute(self):
        saga = (
            Saga("OrdersAdd")
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
            await execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order1"))
        with self.assertRaises(MinosSagaPausedExecutionStepException):
            await execution.execute(reply=reply)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = fake_reply(Foo("order2"))
        context = await execution.execute(reply=reply)

        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext(order1=Foo("order1"), order2=Foo("order2")), context)

    async def test_execute_failure(self):
        saga = (
            Saga("OrdersAdd")
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

    async def test_rollback(self):
        saga = (
            Saga("OrdersAdd")
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


if __name__ == "__main__":
    unittest.main()
