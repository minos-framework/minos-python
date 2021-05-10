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


class TestSagaExecution(unittest.TestCase):
    def test_execute(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order1", lambda order: order)
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply("order2", lambda order: order)
            .step()
            .invoke_participant("Shopping", shipping_callback)
            .with_compensation("BlockOrder", shipping_callback)
            .commit()
        )
        execution = saga.build_execution()

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
            .on_reply("order1", lambda order: order)
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order2", foo_fn_raises)
            .commit()
        )
        execution = saga.build_execution()

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

    def test_raw(self):
        from minos.saga.definitions.step import (
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
                        "raw_invoke_participant": {"callback": create_order_callback, "name": "CreateOrder"},
                        "raw_on_reply": {"callback": identity_fn, "name": "order1"},
                        "raw_with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                    {
                        "raw_invoke_participant": {"callback": create_ticket_callback, "name": "CreateTicket"},
                        "raw_on_reply": {"callback": foo_fn_raises, "name": "order2"},
                        "raw_with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                ],
            },
            "executed_steps": [],
            "status": "created",
            "uuid": "a74d9d6d-290a-492e-afcc-70607958f65d",
        }
        self.assertEqual(expected, execution.raw)

    def test_from_raw(self):
        from minos.saga.definitions.step import (
            identity_fn,
        )

        raw = {
            "already_rollback": False,
            "context": SagaContext(),
            "definition": {
                "name": "OrdersAdd",
                "steps": [
                    {
                        "raw_invoke_participant": {"callback": create_order_callback, "name": "CreateOrder"},
                        "raw_on_reply": {"callback": identity_fn, "name": "order1"},
                        "raw_with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                    {
                        "raw_invoke_participant": {"callback": create_ticket_callback, "name": "CreateTicket"},
                        "raw_on_reply": {"callback": foo_fn_raises, "name": "order2"},
                        "raw_with_compensation": {"callback": delete_order_callback, "name": "DeleteOrder"},
                    },
                ],
            },
            "executed_steps": [],
            "status": "created",
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

        observed = SagaExecution.from_raw(raw)
        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
