import unittest
from collections import (
    namedtuple,
)
from shutil import (
    rmtree,
)
from unittest.mock import (
    AsyncMock,
    call,
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
    MinosSagaFailedExecutionException,
    SagaContext,
    SagaExecution,
    SagaExecutionStorage,
    SagaManager,
    SagaStatus,
)
from tests.utils import (
    ADD_ORDER,
    BASE_PATH,
    DELETE_ORDER,
    FakeHandler,
    FakePool,
    Foo,
    NaiveBroker,
)


class TestSagaManager(unittest.IsolatedAsyncioTestCase):
    DB_PATH = BASE_PATH / "test_db.lmdb"

    def setUp(self) -> None:
        self.config = MinosConfig(BASE_PATH / "config.yml")
        self.broker = NaiveBroker()
        self.handler = FakeHandler("TheReplyTopic")
        self.pool = FakePool(self.handler)
        self.manager = SagaManager.from_config(dynamic_handler_pool=self.pool, config=self.config)

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

    async def test_run_with_pause_on_memory(self):
        send_mock = AsyncMock()
        self.broker.send = send_mock

        reply_topic = "TheReplyTopic"

        Message = namedtuple("Message", ["data"])
        expected_uuid = UUID("a74d9d6d-290a-492e-afcc-70607958f65d")
        with patch("uuid.uuid4", return_value=expected_uuid):
            self.handler.get_one = AsyncMock(
                side_effect=[
                    Message(CommandReply(reply_topic, [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                    Message(CommandReply(reply_topic, [Foo("foo")], expected_uuid, status=CommandStatus.SUCCESS)),
                ]
            )

            execution = await self.manager.run(ADD_ORDER, broker=self.broker)
            self.assertEqual(SagaStatus.Finished, execution.status)
            with self.assertRaises(MinosSagaExecutionNotFoundException):
                self.manager.storage.load(execution.uuid)

        self.assertEqual(2, send_mock.call_count)
        self.assertEqual(
            [
                call(topic="CreateProduct", data=Foo("create_product!"), saga=expected_uuid, reply_topic=reply_topic),
                call(topic="CreateTicket", data=Foo("create_ticket!"), saga=expected_uuid, reply_topic=reply_topic),
            ],
            send_mock.call_args_list,
        )

    async def test_run_with_pause_on_memory_with_error(self):
        self.handler.get_one = AsyncMock(side_effect=ValueError)

        execution = await self.manager.run(ADD_ORDER, broker=self.broker, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)

    async def test_run_with_pause_on_memory_with_error_raises(self):
        self.handler.get_one = AsyncMock(side_effect=ValueError)

        with self.assertRaises(MinosSagaFailedExecutionException):
            await self.manager.run(ADD_ORDER, broker=self.broker)

    async def test_run_with_pause_on_disk(self):
        send_mock = AsyncMock()
        self.broker.send = send_mock

        execution = await self.manager.run(ADD_ORDER, broker=self.broker, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = CommandReply("AddOrderReply", [Foo("foo")], execution.uuid, status=CommandStatus.SUCCESS)
        execution = await self.manager.run(reply=reply, broker=self.broker, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = CommandReply("AddOrderReply", [Foo("foo")], execution.uuid, status=CommandStatus.SUCCESS)
        execution = await self.manager.run(reply=reply, broker=self.broker, pause_on_disk=True)
        with self.assertRaises(MinosSagaExecutionNotFoundException):
            self.manager.storage.load(execution.uuid)

        self.assertEqual(2, send_mock.call_count)
        self.assertEqual(
            [
                call(topic="CreateProduct", data=Foo("create_product!"), saga=execution.uuid, reply_topic=None),
                call(topic="CreateTicket", data=Foo("create_ticket!"), saga=execution.uuid, reply_topic=None),
            ],
            send_mock.call_args_list,
        )

    async def test_run_with_pause_on_disk_returning_uuid(self):
        uuid = await self.manager.run(ADD_ORDER, broker=self.broker, return_execution=False, pause_on_disk=True)
        execution = self.manager.storage.load(uuid)
        self.assertIsInstance(execution, SagaExecution)
        self.assertEqual(SagaStatus.Paused, execution.status)

    async def test_run_with_pause_on_disk_with_context(self):
        context = SagaContext(foo=Foo("foo"), one=1, a="a")

        execution = await self.manager.run(ADD_ORDER, broker=self.broker, context=context, pause_on_disk=True)
        self.assertEqual(context, execution.context)

    async def test_run_with_pause_on_disk_with_error(self):
        execution = await self.manager.run(DELETE_ORDER, broker=self.broker, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = CommandReply("DeleteOrderReply", [Foo("foo")], execution.uuid, status=CommandStatus.SUCCESS)
        execution = await self.manager.run(reply=reply, broker=self.broker, pause_on_disk=True)
        self.assertEqual(SagaStatus.Paused, execution.status)

        reply = CommandReply("DeleteOrderReply", [Foo("foo")], execution.uuid, status=CommandStatus.SUCCESS)
        execution = await self.manager.run(reply=reply, broker=self.broker, pause_on_disk=True, raise_on_error=False)
        self.assertEqual(SagaStatus.Errored, execution.status)


if __name__ == "__main__":
    unittest.main()
