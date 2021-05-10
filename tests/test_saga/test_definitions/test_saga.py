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
    MinosAlreadyOnSagaException,
    MinosSagaException,
    Saga,
    SagaExecution,
    SagaStep,
)
from tests.callbacks import (
    create_ticket_on_reply_callback,
)
from tests.utils import (
    BASE_PATH,
    foo_fn,
)


class TestSaga(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    # noinspection PyMissingOrEmptyDocstring
    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_empty_step_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd2")
                .step()
                .invoke_participant("CreateOrder", foo_fn)
                .with_compensation("DeleteOrder", foo_fn)
                .with_compensation("DeleteOrder2", foo_fn)
                .step()
                .step()
                .invoke_participant("CreateTicket", foo_fn)
                .on_reply("ticket", create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer", foo_fn)
                .commit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_wrong_step_action_must_throw_exception(self):
        with self.assertRaises(MinosSagaException) as exc:
            (
                Saga("OrdersAdd3")
                .step()
                .invoke_participant("CreateOrder", foo_fn)
                .with_compensation("DeleteOrder", foo_fn)
                .with_compensation("DeleteOrder2", foo_fn)
                .step()
                .on_reply("ticket", create_ticket_on_reply_callback)
                .step()
                .invoke_participant("VerifyConsumer", foo_fn)
                .commit()
            )

            self.assertEqual("A 'SagaStep' can only define one 'with_compensation' method.", str(exc))

    def test_build_execution(self):
        saga = (
            Saga("OrdersAdd3")
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .commit()
        )
        execution = saga.build_execution()
        self.assertIsInstance(execution, SagaExecution)

    def test_add_step(self):
        step = SagaStep().invoke_participant("CreateOrder", foo_fn)
        saga = Saga("OrdersAdd3").step(step).commit()

        self.assertEqual([step], saga.steps)

    def test_add_step_raises(self):
        step = SagaStep(Saga("FooTest")).invoke_participant("CreateOrder", foo_fn)
        with self.assertRaises(MinosAlreadyOnSagaException):
            Saga("BarAdd").step(step)

    def test_raw(self):
        saga = (
            Saga("CreateShipment")
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        expected = {
            "name": "CreateShipment",
            "steps": [
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "CreateOrder"},
                    "raw_on_reply": None,
                    "raw_with_compensation": {"callback": foo_fn, "name": "DeleteOrder"},
                },
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "CreateTicket"},
                    "raw_on_reply": {"callback": create_ticket_on_reply_callback, "name": "ticket"},
                    "raw_with_compensation": None,
                },
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "VerifyConsumer"},
                    "raw_on_reply": None,
                    "raw_with_compensation": None,
                },
            ],
        }
        self.assertEqual(expected, saga.raw)

    def test_from_raw(self):
        raw = {
            "name": "CreateShipment",
            "steps": [
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "CreateOrder"},
                    "raw_on_reply": None,
                    "raw_with_compensation": {"callback": foo_fn, "name": "DeleteOrder"},
                },
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "CreateTicket"},
                    "raw_on_reply": {"callback": create_ticket_on_reply_callback, "name": "ticket"},
                    "raw_with_compensation": None,
                },
                {
                    "raw_invoke_participant": {"callback": foo_fn, "name": "VerifyConsumer"},
                    "raw_on_reply": None,
                    "raw_with_compensation": None,
                },
            ],
        }
        expected = (
            Saga("CreateShipment")
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        self.assertEqual(expected, Saga.from_raw(raw))

    def test_from_raw_already(self):
        expected = (
            Saga("CreateShipment")
            .step()
            .invoke_participant("CreateOrder", foo_fn)
            .with_compensation("DeleteOrder", foo_fn)
            .step()
            .invoke_participant("CreateTicket", foo_fn)
            .on_reply("ticket", create_ticket_on_reply_callback)
            .step()
            .invoke_participant("VerifyConsumer", foo_fn)
            .commit()
        )
        self.assertEqual(expected, Saga.from_raw(expected))


if __name__ == "__main__":
    unittest.main()
