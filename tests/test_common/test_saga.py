"""
Copyright (C) 2021 Clariteia SL

This file is part of minos framework.

Minos framework can not be copied and/or distributed without the express permission of Clariteia SL.
"""

import unittest
from unittest.mock import (
    MagicMock,
    call,
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
        self.assertEqual(call("hello", foo="bar"), mock.call_args)

    async def test_reload_and_run(self):
        manager = FakeSagaManager()

        mock = MagicMock(side_effect=manager._load_and_run)
        manager._load_and_run = mock

        reply = CommandReply("hello", [Foo("blue")], "saga_id8972348237", CommandStatus.SUCCESS)

        await manager.run(reply=reply, foo="bar")

        self.assertEqual(1, mock.call_count)
        self.assertEqual(call(reply, foo="bar"), mock.call_args)

    async def test_run_raises(self):

        manager = FakeSagaManager()
        with self.assertRaises(ValueError):
            await manager.run()


if __name__ == "__main__":
    unittest.main()
