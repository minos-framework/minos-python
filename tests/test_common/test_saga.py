import unittest
from unittest.mock import (
    MagicMock,
    call,
)
from uuid import (
    uuid4,
)

from minos.common import (
    CommandReply,
    CommandStatus,
)
from tests.model_classes import (
    Foo,
)
from tests.utils import (
    FakeSagaManager,
)


class TestMinosSagaManager(unittest.IsolatedAsyncioTestCase):
    async def test_run_new(self):
        manager = FakeSagaManager()

        mock = MagicMock(side_effect=manager._run_new)
        manager._run_new = mock

        await manager.run(name="hello", foo="bar")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(name="hello", foo="bar"), mock.call_args)

    async def test_reload_and_run(self):
        manager = FakeSagaManager()

        mock = MagicMock(side_effect=manager._load_and_run)
        manager._load_and_run = mock

        reply = CommandReply("hello", [Foo("blue")], uuid4(), CommandStatus.SUCCESS)

        await manager.run(reply=reply, foo="bar")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(reply, foo="bar"), mock.call_args)


if __name__ == "__main__":
    unittest.main()
