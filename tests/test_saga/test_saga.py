# Copyright (C) 2020 Clariteia SL
#
# This file is part of minos framework.
#
# Minos framework can not be copied and/or distributed without the express
# permission of Clariteia SL.

import unittest
from shutil import rmtree

from minos.saga import (
    MinosSagaException,
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
from tests.utils import BASE_PATH


class TestStringMethods(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_saga_async_callbacks_ok(self):
        s = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("CreateOrder", a_callback)
            .with_compensation("DeleteOrder", b_callback)
            .on_reply(c_callback)
            .execute()
        )

        assert s.get_db_state() is None

    def test_saga_sync_callbacks_ok(self):
        s = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("CreateOrder", d_callback)
            .with_compensation("DeleteOrder", e_callback)
            .on_reply(f_callback)
            .execute()
        )

        assert s.get_db_state() is None

    def test_saga_async_callbacks_ko(self):
        s = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("Shipping", a_callback)
            .with_compensation("DeleteOrder", b_callback)
            .on_reply(c_callback)
            .execute()
        )

        state = s.get_db_state()

        assert state is not None
        assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_saga_sync_callbacks_ko(self):
        s = (
            Saga("OrdersAdd", self.DB_PATH)
            .step()
            .invoke_participant("Shipping", d_callback)
            .with_compensation("DeleteOrder", e_callback)
            .on_reply(f_callback)
            .execute()
        )

        state = s.get_db_state()

        assert state is not None
        assert list(state["operations"].values())[0]["error"] == "invokeParticipantTest exception"

    def test_saga_correct(self):
        s = (
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
            .execute()
        )

        state = s.get_db_state()

        assert state is None

    def test_saga_execute_all_compensations(self):
        s = (
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
            .execute()
        )

        state = s.get_db_state()

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

    def test_saga_empty_step_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd2", self.DB_PATH)
                .step()
                .invoke_participant("CreateOrder")
                .with_compensation("DeleteOrder")
                .with_compensation("DeleteOrder2")
                .step()
                .step()
                .invoke_participant("CreateTicket")
                .on_reply(create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer")
                .execute()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_saga_wrong_step_action_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd3", self.DB_PATH)
                .step()
                .invoke_participant("CreateOrder")
                .with_compensation("DeleteOrder")
                .with_compensation("DeleteOrder2")
                .step()
                .on_reply(create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer")
                .execute()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))


if __name__ == "__main__":
    unittest.main()
