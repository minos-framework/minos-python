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
from uuid import (
    UUID,
)

from minos.common import (
    MinosJsonBinaryProtocol,
    MinosStorageLmdb,
)
from minos.saga import (
    MinosLocalState,
    MinosSagaPausedExecutionStepException,
    Saga,
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
    foo_fn_raises,
)


class TestMinosLocalState(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_add_get(self):
        storage = MinosLocalState(MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        self.assertEqual("bar", storage.get("foo"))

    def test_update_get(self):
        storage = MinosLocalState(MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        storage.update("foo", "foobar")
        self.assertEqual("foobar", storage.get("foo"))

    def test_add_delete(self):
        storage = MinosLocalState(MinosStorageLmdb, db_path=self.DB_PATH)
        storage.add("foo", "bar")
        storage.delete("foo")

    def test_add_get_saga_execution(self):
        storage = MinosLocalState(MinosStorageLmdb, db_path=self.DB_PATH, protocol=MinosJsonBinaryProtocol)

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

        storage.add("foo", expected.raw)
        observed = SagaExecution.from_raw(storage.get("foo"))

        self.assertEqual(expected, observed)


if __name__ == "__main__":
    unittest.main()
