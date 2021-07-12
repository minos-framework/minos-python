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

from minos.common import (
    MinosConfig,
)
from minos.saga import (
    MinosSagaPausedExecutionStepException,
    Saga,
    SagaContext,
    SagaExecution,
)
from tests.callbacks import (
    create_order_callback,
    create_ticket_callback,
    delete_order_callback,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    NaiveBroker,
    fake_reply,
    foo_fn_raises,
)


class TestSagaExecution(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.saga = (
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

    def setUp(self) -> None:
        self.config = MinosConfig(path=BASE_PATH / "config.yml")
        self.broker = NaiveBroker()

        self.publish_mock = MagicMock(side_effect=self.broker.send)
        self.broker.send = self.publish_mock

    def test_from_raw(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
        observed = SagaExecution.from_raw(expected)
        self.assertEqual(expected, observed)

    def test_created(self):
        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            execution = SagaExecution.from_saga(self.saga)

        expected = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "name": "OrdersAdd",
                "commit_callback": "minos.saga.definitions.step.identity_fn",
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
            "paused_step": None,
            "status": "created",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }
        observed = execution.raw
        self.assertEqual(
            SagaContext.from_avro_str(expected.pop("context")), SagaContext.from_avro_str(observed.pop("context"))
        )
        self.assertEqual(expected, observed)

    async def test_partial_step(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext().avro_str,
            "definition": {
                "name": "OrdersAdd",
                "commit_callback": "minos.saga.definitions.step.identity_fn",
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
            "paused_step": {
                "definition": {
                    "invoke_participant": {"callback": "tests.callbacks.create_order_callback", "name": "CreateOrder"},
                    "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "order1"},
                    "with_compensation": {"callback": "tests.callbacks.delete_order_callback", "name": "DeleteOrder"},
                },
                "status": "paused-on-reply",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(broker=self.broker)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)

    async def test_executed_step(self):
        raw = {
            "already_rollback": False,
            "context": SagaContext(order1=Foo("hola")).avro_str,
            "definition": {
                "name": "OrdersAdd",
                "commit_callback": "minos.saga.definitions.step.identity_fn",
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
            "paused_step": {
                "definition": {
                    "invoke_participant": {"callback": "tests.callbacks.create_order_callback", "name": "CreateOrder"},
                    "on_reply": {"callback": "minos.saga.definitions.step.identity_fn", "name": "order1"},
                    "with_compensation": {"callback": "tests.callbacks.delete_order_callback", "name": "DeleteOrder"},
                },
                "status": "paused-on-reply",
                "already_rollback": False,
            },
            "status": "paused",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }

        with patch("uuid.uuid4", return_value=UUID("a74d9d6d-290a-492e-afcc-70607958f65d")):
            expected = SagaExecution.from_saga(self.saga)
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(broker=self.broker)

            reply = fake_reply(Foo("hola"))
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                await expected.execute(reply=reply, broker=self.broker)

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
