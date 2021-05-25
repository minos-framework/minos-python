"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""
import unittest
from unittest.mock import (
    MagicMock,
    patch,
)
from uuid import (
    UUID,
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
        self.assertEqual(3, self.publish_mock.call_count)

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

    def test_raw(self):
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
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_saga(saga)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "name": "OrdersAdd",
                "steps": [
                    {
                        "invoke_participant": {
                            "callback": "tests.callbacks.create_order_callback",
                            "name": "CreateOrder",
                        },
                        "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "order1"},
                        "with_compensation": {
                            "callback": "tests.callbacks.delete_order_callback",
                            "name": "DeleteOrder",
                        },
                    },
                    {
                        "invoke_participant": {
                            "callback": "tests.callbacks.create_ticket_callback",
                            "name": "CreateTicket",
                        },
                        "on_reply": {"callback": "tests.utils.foo_fn_raises", "name": "order2"},
                        "with_compensation": {
                            "callback": "tests.callbacks.delete_order_callback",
                            "name": "DeleteOrder",
                        },
                    },
                ],
            },
            "executed_steps": [],
            "status": "created",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }
        observed = execution.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("context")), SagaContext.from_avro_str(observed.pop("context"))
        )
        self.assertEqual(expected, observed)

    async def test_from_raw(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext(order1=Foo("hola")).avro_str,
            "definition": {
                "name": "OrdersAdd",
                "steps": [
                    {
                        "invoke_participant": {
                            "callback": "tests.callbacks.create_order_callback",
                            "name": "CreateOrder",
                        },
                        "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "order1"},
                        "with_compensation": {
                            "callback": "tests.callbacks.delete_order_callback",
                            "name": "DeleteOrder",
                        },
                    },
                    {
                        "invoke_participant": {
                            "callback": "tests.callbacks.create_ticket_callback",
                            "name": "CreateTicket",
                        },
                        "on_reply": {"callback": "tests.utils.foo_fn_raises", "name": "order2"},
                        "with_compensation": {
                            "callback": "tests.callbacks.delete_order_callback",
                            "name": "DeleteOrder",
                        },
                    },
                ],
            },
            "executed_steps": [
                {
                    "definition": {
                        "invoke_participant": {
                            "callback": "tests.callbacks.create_order_callback",
                            "name": "CreateOrder",
                        },
                        "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "order1"},
                        "with_compensation": {
                            "callback": "tests.callbacks.delete_order_callback",
                            "name": "DeleteOrder",
                        },
                    },
                    "status": "finished",
                    "already_rollback": False,
                }
            ],
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

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
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(saga)
            try:
                await expected.execute()
            except MinosSagaPausedExecutionStepException:
                pass
            try:
                reply = fake_reply(Foo("hola"))
                await expected.execute(reply=reply)
            except MinosSagaPausedExecutionStepException:
                pass

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    def test_from_raw_already(self):
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
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(saga)
        self.assertEqual(expected, SagaExecution.from_raw(expected))


if __name__ == "__main__":
    unittest.main()
