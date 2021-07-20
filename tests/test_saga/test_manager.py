"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from collections import (
    namedtuple,
)
from shutil import (
    rmtree,
)
from unittest.mock import (
    AsyncMock,
    patch,
)
from uuid import (
    UUID,
)

from minos.common import (
    CommandReply,
    CommandStatus,
    MinosConfig,
    MinosHandlerNotProvidedException,
    MinosSagaManager,
)
from minos.saga import (
    MinosSagaExecutionNotFoundException,
    SagaContext,
    SagaExecutionStorage,
    SagaManager,
    SagaStatus,
)
from tests.utils import (
    BASE_PATH,
    FakeHandler,
    Foo,
    NaiveBroker,
)


class TestSagaManager(unittest.IsolatedAsyncioTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def setUp(self) -> None:
        self.config = MinosConfig(BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.handler = FakeHandler()
        self.manager = SagaManager.from_config(handler=self.handler, config=self.config)

    def tearDown(self) -> None:
        rmtree(self.DB_PATH, ignore_errors=True)

    def test_constructor(self):
        self.assertIsInstance(self.manager.storage, SagaExecutionStorage)
        self.assertIsInstance(self.manager, MinosSagaManager)

    def test_constructor_without_handler(self):
        with self.assertRaises(MinosHandlerNotProvidedException):
            SagaManager.from_config(handler=None, config=self.config)

    async def test_context_manager(self):
        async with self.manager as saga_manager:
            self.assertIsInstance(saga_manager, SagaManager)

    async def test_run(self):
        uuid = await self.manager.run("AddOrder", broker=self.broker)
        self.assertEqual(SagaStatus.Paused, self.manager.storage.load(uuid).status)

        reply = CommandReply("AddOrderReply", [Foo("foo")], str(uuid), status=CommandStatus.SUCCESS)
        await self.manager.run(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, self.manager.storage.load(uuid).status)

        reply = CommandReply("AddOrderReply", [Foo("foo")], str(uuid), status=CommandStatus.SUCCESS)
        await self.manager.run(reply=reply, broker=self.broker)
        with self.assertRaises(MinosSagaExecutionNotFoundException):
            self.manager.storage.load(uuid)

    async def test_run_not_asynchronous(self):
        Message = namedtuple("Message", ["data"])
        expected_uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")
        with patch("uuid.uuid4", return_value=expected_uuid):
            self.handler.get_one = AsyncMock(
                side_effect=[
                    Message(CommandReply("AddOrderReply", [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                    Message(CommandReply("AddOrderReply", [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                ]
            )

            observed_uuid = await self.manager.run("AddOrder", broker=self.broker, asynchronous=False)
            self.assertEqual(expected_uuid, observed_uuid)
            with self.assertRaises(MinosSagaExecutionNotFoundException):
                self.manager.storage.load(observed_uuid)

    async def test_run_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")

        uuid = await self.manager.run("AddOrder", broker=self.broker, context=context)
        self.assertEqual(context, self.manager.storage.load(uuid).context)

    async def test_run_with_error(self):
        uuid = await self.manager.run("DeleteOrder", broker=self.broker)
        self.assertEqual(SagaStatus.Paused, self.manager.storage.load(uuid).status)

        reply = CommandReply("DeleteOrderReply", [Foo("foo")], str(uuid), status=CommandStatus.SUCCESS)
        await self.manager.run(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Paused, self.manager.storage.load(uuid).status)

        reply = CommandReply("DeleteOrderReply", [Foo("foo")], str(uuid), status=CommandStatus.SUCCESS)
        await self.manager.run(reply=reply, broker=self.broker)
        self.assertEqual(SagaStatus.Errored, self.manager.storage.load(uuid).status)


if __name__ == "__main__":
    unittest.main()
