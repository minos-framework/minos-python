"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from shutil import (
    rmtree,
)

from minos.saga import (
    MinosSagaExecutionNotFoundException,
    MinosSagaPausedExecutionStepException,
    Saga,
    SagaExecution,
    SagaExecutionStorage,
)
from tests.callbacks import (
    create_order_callback,
    create_ticket_callback,
    delete_order_callback,
)
from tests.utils import (
    BASE_PATH,
    Foo,
    foo_fn_raises,
)


class TestMinosLocalState(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def setUp(self) -> None:
        self.saga = (
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

        execution = SagaExecution.from_saga(self.saga)
        try:
            execution.execute()
        except MinosSagaPausedExecutionStepException:
            pass
        try:
            execution.execute(response=Foo("hola"))
        except MinosSagaPausedExecutionStepException:
            pass
        self.execution = execution

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_store(self):
        storage = SagaExecutionStorage(self.DB_PATH)

        storage.store(self.execution)

        self.assertEqual(self.execution, storage.load(self.execution.uuid))

    def test_store_ovewrite(self):
        storage = SagaExecutionStorage(self.DB_PATH)

        storage.store(self.execution)
        self.assertEqual(self.execution, storage.load(self.execution.uuid))

        another = SagaExecution.from_saga(self.saga)
        another.uuid = self.execution.uuid
        storage.store(another)

        self.assertNotEqual(self.execution, storage.load(self.execution.uuid))
        self.assertEqual(another, storage.load(self.execution.uuid))

    def test_load_raises(self):
        storage = SagaExecutionStorage(self.DB_PATH)

        with self.assertRaises(MinosSagaExecutionNotFoundException):
            storage.load(self.execution.uuid)

    def test_delete(self):
        storage = SagaExecutionStorage(self.DB_PATH)

        storage.store(self.execution)
        storage.delete(self.execution.uuid)
        with self.assertRaises(MinosSagaExecutionNotFoundException):
            storage.load(self.execution.uuid)


if __name__ == "__main__":
    unittest.main()
