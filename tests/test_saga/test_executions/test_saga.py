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
    MinosSagaStorage,
    Saga,
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

    def test_correct(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply("order", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply("order", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("Shopping", shipping_callback)
            .with_compensation("BlockOrder", shipping_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with MinosSagaStorage.from_execution(execution) as storage:
            state = storage.get_state()


if __name__ == "__main__":
    unittest.main()
