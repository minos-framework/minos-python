import unittest
from datetime import (
    datetime,
)

from minos.common import (
    CommandReply,
)
from minos.common.testing import (
    PostgresAsyncTestCase,
)
from minos.networks import (
    CommandReplyHandler,
    HandlerEntry,
)
from tests.utils import (
    BASE_PATH,
    FakeModel,
    FakeSagaManager,
)


class TestCommandReplyHandler(PostgresAsyncTestCase):
    CONFIG_FILE_PATH = BASE_PATH / "test_config.yml"

    def test_from_config(self):
        dispatcher = CommandReplyHandler.from_config(config=self.config)
        self.assertIsInstance(dispatcher, CommandReplyHandler)

    def test_entry_model_cls(self):
        self.assertEqual(CommandReply, CommandReplyHandler.ENTRY_MODEL_CLS)

    async def test_dispatch(self):
        saga_manager = FakeSagaManager()

        command = CommandReply("TicketAdded", [FakeModel("foo")], saga_uuid="43434jhij", reply_on="mkk2334")
        entry = HandlerEntry(1, "TicketAdded", None, 0, command, 1, datetime.now())

        async with CommandReplyHandler.from_config(config=self.config, saga_manager=saga_manager) as handler:
            await handler.dispatch_one(entry)

        self.assertEqual(None, saga_manager.name)
        self.assertEqual(command, saga_manager.reply)


if __name__ == "__main__":
    unittest.main()
