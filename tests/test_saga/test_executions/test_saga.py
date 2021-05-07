"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from shutil import (
    rmtree,
)
from unittest.mock import (
    patch,
)

from minos.saga import (
    MinosSagaFailedExecutionStepException,
    MinosSagaPausedExecutionStepException,
    MinosSagaStorage,
    Saga,
    SagaContext,
    SagaStatus,
)
from tests.callbacks import (
    create_order_callback,
    create_ticket_callback,
    create_ticket_on_reply_callback,
    d_callback,
    delete_order_callback,
    e_callback,
    f_callback,
    shipping_callback,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    foo_fn_raises,
)


class TestSagaExecution(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_get_state(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("CreateOrder", d_callback)
            .with_compensation("DeleteOrder", e_callback)
            .on_reply("order", f_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with MinosSagaStorage.from_execution(execution) as storage:
            observed = storage.get_state()

        expected = {"current_step": None, "operations": {}, "saga": "OrdersAdd"}
        self.assertEqual(expected, observed)

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
        execution = saga.build_execution(self.DB_PATH)
        with MinosSagaStorage.from_execution(execution) as storage:
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                execution.execute(storage)
            self.assertEqual(SagaStatus.Paused, execution.status)

            with self.assertRaises(MinosSagaPausedExecutionStepException):
                execution.execute(storage, response=Foo("order1"))
            self.assertEqual(SagaStatus.Paused, execution.status)

            context = execution.execute(storage, response=Foo("order2"))
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
        execution = saga.build_execution(self.DB_PATH)
        with MinosSagaStorage.from_execution(execution) as storage:
            with self.assertRaises(MinosSagaPausedExecutionStepException):
                execution.execute(storage)
            self.assertEqual(SagaStatus.Paused, execution.status)

            with self.assertRaises(MinosSagaPausedExecutionStepException):
                execution.execute(storage, response=Foo("order1"))
            self.assertEqual(SagaStatus.Paused, execution.status)

            with patch("minos.saga.executions.executors.with_compensation.WithCompensationExecutor.publish") as mock:
                with self.assertRaises(MinosSagaFailedExecutionStepException):
                    execution.execute(storage, response=Foo("order2"))
                self.assertEqual(SagaStatus.Errored, execution.status)
                self.assertEqual(2, mock.call_count)


if __name__ == "__main__":
    unittest.main()
