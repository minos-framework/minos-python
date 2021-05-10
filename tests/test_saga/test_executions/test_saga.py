"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
import uuid
from unittest.mock import (
    patch,
)
from uuid import (
    UUID,
)

from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
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
    Foo,
    foo_fn_raises,
)

_PUBLISH_MOCKER = patch("minos.saga.executions.executors.publish.PublishExecutor.publish")


class TestSagaExecution(unittest.TestCase):
    def test_execute(self):
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
            execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            execution.execute(response=Foo("order1"))
        self.assertEqual(SagaStatus.Paused, execution.status)

        context = execution.execute(response=Foo("order2"))
        self.assertEqual(SagaStatus.Finished, execution.status)
        self.assertEqual(SagaContext({"order1": Foo("order1"), "order2": Foo("order2")}), context)

    def test_execute_failure(self):
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
            execution.execute()
        self.assertEqual(SagaStatus.Paused, execution.status)

        with self.assertRaises(MinosSagaPausedExecutionStepException):
            execution.execute(response=Foo("order1"))
        self.assertEqual(SagaStatus.Paused, execution.status)

        with patch("minos.saga.executions.executors.with_compensation.WithCompensationExecutor.publish") as mock:
            with self.assertRaises(MinosSagaFailedExecutionStepException):
                execution.execute(response=Foo("order2"))
            self.assertEqual(SagaStatus.Errored, execution.status)
            self.assertEqual(2, mock.call_count)

    def test_rollback(self):
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
            execution.execute()
        execution.execute(response=Foo("order1"))

        with _PUBLISH_MOCKER as mock:
            execution.rollback()
            self.assertEqual(1, mock.call_count)

            mock.reset_mock()
            execution.rollback()
            self.assertEqual(0, mock.call_count)

    def test_raw(self):
        from minos.saga import (
            identity_fn,
        )

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
            "context": SagaContext(),
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
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):
        from minos.saga import (
            identity_fn,
        )

        raw = {
            "already_rollback": False,
            "context": SagaContext({"order1": Foo("hola")}),
            "definition": {
                "name": "OrdersAdd",
                "steps": [
                    {
                        "invoke_participant": {"callback": create_order_callback, "name": "CreateOrder"},
                        "on_reply": {"callback": identity_fn, "name": "order1"},
                        "with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                    {
                        "invoke_participant": {"callback": create_ticket_callback, "name": "CreateTicket"},
                        "on_reply": {"callback": foo_fn_raises, "name": "order2"},
                        "with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                ],
            },
            "executed_steps": [
                {
                    "definition": {
                        "invoke_participant": {"name": "CreateOrder", "callback": create_order_callback},
                        "with_compensation": {"name": "DeleteOrder", "callback": delete_order_callback},
                        "on_reply": {"name": "order1", "callback": identity_fn},
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
                expected.execute()
            except MinosSagaPausedExecutionStepException:
                pass
            try:
                expected.execute(response=Foo("hola"))
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
