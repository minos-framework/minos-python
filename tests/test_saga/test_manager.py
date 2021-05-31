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
    NaiveBroker,
)


class TestSagaManager(unittest.IsolatedAsyncioTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def setUp(self) -> None:
        self.config = MinosConfig(BASE_PATH / "config.yml")
        self.broker = NaiveBroker()

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_constructor(self):
        manager = SagaManager.from_config(config=self.config)
        self.assertIsInstance(manager.storage, SagaExecutionStorage)

    async def test_context_manager(self):
        async with SagaManager.from_config(config=self.config) as saga_manager:
            self.assertIsInstance(saga_manager, SagaManager)

    async def test_run_ok(self):
        manager = SagaManager.from_config(config=self.config)

        uuid = await manager.run("AddOrder", broker=self.broker)
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        await manager.run(reply=CommandReply("AddOrderReply", [Foo("foo")], str(uuid)), broker=self.broker)
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        await manager.run(reply=CommandReply("AddOrderReply", [Foo("foo")], str(uuid)), broker=self.broker)
        with self.assertRaises(MinosSagaExecutionNotFoundException):
            manager.storage.load(uuid)

    async def test_run_err(self):
        manager = SagaManager.from_config(config=self.config)

        uuid = await manager.run("DeleteOrder", broker=self.broker)
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        await manager.run(reply=CommandReply("DeleteOrderReply", [Foo("foo")], str(uuid)), broker=self.broker)
        self.assertEqual(SagaStatus.Paused, manager.storage.load(uuid).status)

        await manager.run(reply=CommandReply("DeleteOrderReply", [Foo("foo")], str(uuid)), broker=self.broker)
        self.assertEqual(SagaStatus.Errored, manager.storage.load(uuid).status)


if __name__ == "__main__":
    unittest.main()
