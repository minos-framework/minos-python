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

    def test_async_callbacks_ok(self):
        with patch("uuid.uuid4", side_effect=["uuid-0", "uuid-1", "uuid-2", "uuid-3", "uuid-4", "uuid-5"]):
            saga = (
                Saga("OrdersAdd", self.DB_PATH)
                .step()
                .invoke_participant("CreateOrder", a_callback)
                .with_compensation("DeleteOrder", b_callback)
                .on_reply(c_callback)
                .commit()
            )
            with saga.step_manager as step_manager:
                execution = saga.build_execution()
                execution.execute(step_manager)
                observed = step_manager.get_state()

        expected = {
            "current_step": "uuid-5",
            "operations": {
                "uuid-1": {
                    "error": "",
                    "id": "uuid-1",
                    "name": "CreateOrder",
                    "response": "_invokeParticipant Response",
                    "status": 0,
                    "type": "invokeParticipant",
                },
                "uuid-4": {
                    "error": "",
                    "id": "uuid-4",
                    "name": "CreateOrder",
                    "response": "create_order_callback response!!!!",
                    "status": 0,
                    "type": "invokeParticipant_callback",
                },
                "uuid-5": {
                    "error": "",
                    "id": "uuid-5",
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
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("Shipping", a_callback)
            .with_compensation("DeleteOrder", b_callback)
            .on_reply(c_callback)
            .commit()
        )
        with saga.step_manager as step_manager:
            execution = saga.build_execution()
            execution.execute(step_manager)

            state = step_manager.get_state()

            assert state is not None
            assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_sync_callbacks_ko(self):
        saga = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("Shipping", d_callback)
            .with_compensation("DeleteOrder", e_callback)
            .on_reply(f_callback)
            .commit()
        )
        with saga.step_manager as step_manager:
            execution = saga.build_execution()
            execution.execute(step_manager)

            state = step_manager.get_state()

            assert state is not None
            assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_correct(self):
        saga = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("CreateTicket", create_ticket_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("Shopping")
            .with_compensation(["Failed", "BlockOrder"], shipping_callback)
            .commit()
        )
        with saga.step_manager as step_manager:
            state = step_manager.get_state()

        self.assertEqual({"current_step": None, "operations": {}, "saga": "OrdersAdd"}, state)

    def test_execute_all_compensations(self):
        saga = (
            Saga("ItemsAdd", self.DB_PATH)
            .step()
            .invoke_participant("CreateOrder", create_order_callback)
            .with_compensation("DeleteOrder", delete_order_callback)
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("CreateTicket")
            .on_reply(create_ticket_on_reply_callback)
            .step()
            .invoke_participant("Shipping")
            .with_compensation(["Failed", "BlockOrder"], shipping_callback)
            .commit()
        )
        with saga.step_manager as step_manager:
            execution = saga.build_execution()
            execution.execute(step_manager)

            state = step_manager.get_state()

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
