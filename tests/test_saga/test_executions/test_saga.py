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
    Saga,
)
from tests.callbacks import (
    a_callback,
    b_callback,
    c_callback,
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
            .on_reply(f_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with execution.storage as storage:
            observed = storage.get_state()

        expected = {"current_step": None, "operations": {}, "saga": "OrdersAdd"}
        self.assertEqual(expected, observed)

    def test_async_callbacks_ok(self):
        with patch("uuid.uuid4", side_effect=["uuid-0", "uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5"]):
            saga = (
                Saga("OrdersAdd")
                .step()
                .invoke_participant("CreateOrder", a_callback)
                .with_compensation("DeleteOrder", b_callback)
                .on_reply(c_callback)
                .commit()
            )
            execution = saga.build_execution(self.DB_PATH)
            with execution.storage as storage:
                execution.execute(storage)
                observed = storage.get_state()

        expected = {
            "current_step": "uuid-4",
            "operations": {
                "uuid-0": {
                    "error": "",
                    "id": "uuid-0",
                    "name": "CreateOrder",
                    "response": "_invokeParticipant Response",
                    "status": 0,
                    "type": "invokeParticipant",
                },
                "uuid-3": {
                    "error": "",
                    "id": "uuid-3",
                    "name": "CreateOrder",
                    "response": "create_order_callback response!!!!",
                    "status": 0,
                    "type": "invokeParticipant_callback",
                },
                "uuid-4": {
                    "error": "",
                    "id": "uuid-4",
                    "name": "",
                    "response": "async create_ticket_on_reply_callback " "response!!!!",
                    "status": 0,
                    "type": "onReply",
                },
            },
            "saga": "OrdersAdd",
        }
        self.assertEqual(expected, observed)

    def test_async_callbacks_ko(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("Shipping", a_callback)
            .with_compensation("DeleteOrder", b_callback)
            .on_reply(c_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with execution.storage as storage:
            execution.execute(storage)

            state = storage.get_state()

            assert state is not None
            assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_sync_callbacks_ko(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("Shipping", d_callback)
            .with_compensation("DeleteOrder", e_callback)
            .on_reply(f_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with execution.storage as storage:
            execution.execute(storage)

            state = storage.get_state()

            assert state is not None
            assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_correct(self):
        saga = (
            Saga("OrdersAdd")
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("Shopping")
            .with_compensation("BlockOrder", shipping_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with execution.storage as storage:
            state = storage.get_state()

        self.assertEqual({"current_step": None, "operations": {}, "saga": "OrdersAdd"}, state)

    def test_execute_all_compensations(self):
        saga = (
            Saga("ItemsAdd")
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("CreateTicket")
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("Shipping")
            .with_compensation("BlockOrder", shipping_callback)
            .commit()
        )
        execution = saga.build_execution(self.DB_PATH)
        with execution.storage as storage:
            execution.execute(storage)

            state = storage.get_state()

            assert state is not None
            assert list(state["operations"].values())[0]["type"] == "invokeParticipant"
            assert list(state["operations"].values())[1]["type"] == "invokeParticipant_callback"
            assert list(state["operations"].values())[2]["type"] == "onReply"
            assert list(state["operations"].values())[3]["type"] == "invokeParticipant"
            assert list(state["operations"].values())[4]["type"] == "onReply"
            assert list(state["operations"].values())[5]["type"] == "invokeParticipant"
            assert list(state["operations"].values())[6]["type"] == "withCompensation"
            assert list(state["operations"].values())[7]["type"] == "withCompensation_callback"
            assert list(state["operations"].values())[8]["type"] == "withCompensation"
            assert list(state["operations"].values())[9]["type"] == "withCompensation_callback"


if __name__ == "__main__":
    unittest.main()
