"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from shutil import (
    rmtree,
)

from minos.common import (
    CommandReply,
    MinosConfig,
)
from minos.saga import (
    MinosSagaExecutionNotFoundException,
    SagaExecutionStorage,
    SagaManager,
    SagaStatus,
)
from tests.utils import (
    BASE_PATH,
    Foo,
)


class TestSagaManager(unittest.TestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def setUp(self) -> None:
        self.config = MinosConfig(BASE_PATH / "config.yml")

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_constructor(self):
        manager = SagaManager.from_config(config=self.config)
        self.assertIsInstance(manager.storage, SagaExecutionStorage)

    def test_run_ok(self):
        manager = SagaManager.from_config(config=self.config)

        uuid = manager.run("AddOrder")
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        manager.run(reply=CommandReply("AddOrderReply", [Foo("foo")], "AddOrder", str(uuid)))
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        manager.run(reply=CommandReply("AddOrderReply", [Foo("foo")], "AddOrder", str(uuid)))
        with self.assertRaises(MinosSagaExecutionNotFoundException):
            manager.storage.load(uuid)

    def test_run_err(self):
        manager = SagaManager.from_config(config=self.config)

        uuid = manager.run("DeleteOrder")
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        manager.run(reply=CommandReply("DeleteOrderReply", [Foo("foo")], "DeleteOrder", str(uuid)))
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        manager.run(reply=CommandReply("DeleteOrderReply", [Foo("foo")], "DeleteOrder", str(uuid)))
        self.assertEqual(SagaStatus.Errored, manager.storage.load(uuid).status)


if __name__ == "__main__":
    unittest.main()
