import unittest
from datetime import (
    datetime,
)
from unittest.mock import (
    AsyncMock,
)

from minos.common import (
    Command,
    Response,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandHandler,
    CommandRequest,
    HandlerEntry,
)
from tests.utils import (
    BASE_PATH,
    FakeBroker,
    FakeModel,
)


class TestCommandHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        broker = FakeBroker()
        dispatcher = CommandHandler.from_config(config=self.config, broker=broker)
        self.assertIsInstance(dispatcher, CommandHandler)
        self.assertEqual(
            {
                "AddOrder": {"action": "add_order", "controller": "tests.services.CommandTestService.CommandService"},
                "DeleteOrder": {
                    "action": "delete_order",
                    "controller": "tests.services.CommandTestService.CommandService",
                },
                "GetOrder": {"action": "get_order", "controller": "tests.service.CommandTestService.CommandService"},
                "UpdateOrder": {
                    "action": "update_order",
                    "controller": "tests.services.CommandTestService.CommandService",
                },
            },
            dispatcher._handlers,
        )
        self.assertEqual(self.config.commands.queue.records, dispatcher._records)
        self.assertEqual(self.config.commands.queue.retry, dispatcher._retry)
        self.assertEqual(self.config.commands.queue.host, dispatcher.host)
        self.assertEqual(self.config.commands.queue.port, dispatcher.port)
        self.assertEqual(self.config.commands.queue.database, dispatcher.database)
        self.assertEqual(self.config.commands.queue.user, dispatcher.user)
        self.assertEqual(self.config.commands.queue.password, dispatcher.password)
        self.assertEqual(broker, dispatcher.broker)

    def test_entry_model_cls(self):
        self.assertEqual(Command, CommandHandler.ENTRY_MODEL_CLS)

    async def test_dispatch(self):
        mock = AsyncMock(return_value=Response("add_order"))
        broker = FakeBroker()

        reply = Command(topic="AddOrder", items=[FakeModel("foo")], saga_uuid="43434jhij", reply_topic="UpdateTicket")
        entry = HandlerEntry(1, "AddOrder", mock, 0, reply, 1, datetime.now())

        async with CommandHandler.from_config(config=self.config, broker=broker) as handler:
            await handler.dispatch_one(entry)

        self.assertEqual(1, broker.call_count)
        self.assertEqual(["add_order"], broker.items)
        self.assertEqual("UpdateTicket", broker.topic)
        self.assertEqual("43434jhij", broker.saga_uuid)
        self.assertEqual(None, broker.reply_topic)

        self.assertEqual(1, mock.call_count)
        observed = mock.call_args[0][0]
        self.assertIsInstance(observed, CommandRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())

    async def test_dispatch_without_reply(self):
        mock = AsyncMock()
        broker = FakeBroker()

        reply = Command(topic="AddOrder", items=[FakeModel("foo")], saga_uuid="43434jhij")
        entry = HandlerEntry(1, "AddOrder", mock, 0, reply, 1, datetime.now())

        async with CommandHandler.from_config(config=self.config, broker=broker) as handler:
            await handler.dispatch_one(entry)

        self.assertEqual(0, broker.call_count)

        self.assertEqual(1, mock.call_count)
        observed = mock.call_args[0][0]
        self.assertIsInstance(observed, CommandRequest)
        self.assertEqual(FakeModel("foo"), await observed.content())


if __name__ == "__main__":
    unittest.main()
